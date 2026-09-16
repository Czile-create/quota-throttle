//! new-api 原生进程托管：按平台下载 release 二进制、sha256 校验、detached 启动、健康检查、停止。
//!
//! 不用容器：new-api 就是一个 Go 单二进制 + 内置 SQLite。我们把它下到 data_dir 里跑，
//! 进程放进独立进程组（不与本守护父子耦合），PID 落盘，便于 `down` 停。

use crate::config::ManageConfig;
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;
use tracing::{info, warn};

/// 平台对应的 release 资产名与 checksums 文件名。
fn asset_names(version: &str) -> Result<(String, &'static str)> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let (asset, checksums) = match (os, arch) {
        ("linux", "x86_64") => (format!("new-api-{version}"), "checksums-linux.txt"),
        ("linux", "aarch64") => (format!("new-api-arm64-{version}"), "checksums-linux.txt"),
        ("macos", _) => (format!("new-api-macos-{version}"), "checksums-macos.txt"),
        _ => bail!("不支持的平台 {os}/{arch}：请手动部署 new-api 或改用 run 子命令"),
    };
    Ok((asset, checksums))
}

pub struct NewApiProcess {
    pub data_dir: PathBuf,
    pub binary: PathBuf,
    pub port: u16,
    pub base_url: String,
    repo: String,
    version: String,
    client: reqwest::Client,
}

impl NewApiProcess {
    pub fn new(cfg: &ManageConfig, base_url: &str) -> Result<Self> {
        let (asset, _) = asset_names(&cfg.version)?;
        let data_dir = PathBuf::from(&cfg.data_dir);
        Ok(Self {
            binary: data_dir.join(&asset),
            data_dir,
            port: cfg.port,
            base_url: base_url.trim_end_matches('/').to_string(),
            repo: cfg.repo.clone(),
            version: cfg.version.clone(),
            client: reqwest::Client::new(),
        })
    }

    fn pid_file(&self) -> PathBuf {
        self.data_dir.join("new-api.pid")
    }

    /// 确保二进制存在：不在就下载并校验 sha256。
    pub async fn ensure_binary(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("创建 data_dir 失败: {}", self.data_dir.display()))?;
        if self.binary.exists() {
            info!(binary = %self.binary.display(), "new-api 二进制已存在，跳过下载");
            return Ok(());
        }

        let (asset, checksums_file) = asset_names(&self.version)?;
        let base = format!(
            "https://github.com/{}/releases/download/{}",
            self.repo, self.version
        );

        // 先取期望 sha256
        let checksums_url = format!("{base}/{checksums_file}");
        info!(url = %checksums_url, "下载 checksums");
        let checksums = self
            .client
            .get(&checksums_url)
            .send()
            .await
            .context("下载 checksums 失败")?
            .error_for_status()
            .context("checksums 响应非 2xx")?
            .text()
            .await?;
        let want = checksums
            .lines()
            .find_map(|l| {
                let mut it = l.split_whitespace();
                let sha = it.next()?;
                let name = it.next()?;
                (name == asset).then(|| sha.to_string())
            })
            .with_context(|| format!("checksums 里找不到 {asset}"))?;

        // 下载二进制（约 130MB，一次性 buffer）
        let bin_url = format!("{base}/{asset}");
        info!(url = %bin_url, "下载 new-api 二进制（约 130MB，稍等）");
        let bytes = self
            .client
            .get(&bin_url)
            .send()
            .await
            .context("下载 new-api 二进制失败")?
            .error_for_status()
            .context("二进制响应非 2xx")?
            .bytes()
            .await
            .context("读取二进制响应体失败")?;

        let got = hex(&Sha256::digest(&bytes));
        if got != want {
            bail!("sha256 校验不通过: 期望 {want} 实得 {got}");
        }
        info!("sha256 校验通过");

        let tmp = self.binary.with_extension("part");
        std::fs::write(&tmp, &bytes).context("写入二进制失败")?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
            .context("chmod 失败")?;
        std::fs::rename(&tmp, &self.binary).context("重命名二进制失败")?;
        info!(binary = %self.binary.display(), "new-api 就绪");
        Ok(())
    }

    /// GET {base}/api/status，200 即视为健康。
    pub async fn is_healthy(&self) -> bool {
        let url = format!("{}/api/status", self.base_url);
        matches!(
            self.client
                .get(&url)
                .timeout(Duration::from_secs(3))
                .send()
                .await,
            Ok(r) if r.status().is_success()
        )
    }

    /// 确保 new-api 在跑：已健康则直接返回；否则 detached 启动并等到健康。
    pub async fn ensure_running(&self) -> Result<()> {
        if self.is_healthy().await {
            info!(base = %self.base_url, "new-api 已在运行");
            return Ok(());
        }
        // **双进程防护（F4 端口迁移首日必踩）**：本工具托管的旧进程还活着（比如还占着
        // 3000，而 upstream 已改成 13000）→ 直接再拉一个会双进程抢同一个 SQLite。
        // PID 文件只属于本工具起的进程——先停掉它（**等它真正退场**再启动，PR review #13：
        // SIGTERM 是异步的，旧进程还在 flush SQLite/占着端口时新进程就起 = 双写者 +
        // 代理 bind 假失败）。
        let pf = self.pid_file();
        if let Ok(pid) = std::fs::read_to_string(&pf) {
            let pid: i32 = pid.trim().parse().unwrap_or(-1);
            if process_alive(pid) && process_is_newapi(pid) {
                warn!(pid, "托管的新旧 new-api 进程还活着但 upstream 不健康——先停掉再启动（防双进程抢同一 SQLite）");
                self.stop()?;
                wait_exit(pid, Duration::from_secs(10)).await;
            }
        }
        self.ensure_binary().await?;

        let log_path = self.data_dir.join("new-api.log");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .context("打开 new-api 日志失败")?;
        let log_err = log.try_clone()?;

        // 绝对路径启动，工作目录设为 data_dir，让 SQLite/日志都落这里。
        let binary_abs = std::fs::canonicalize(&self.binary).context("解析二进制绝对路径失败")?;
        let data_abs = std::fs::canonicalize(&self.data_dir)?;

        let child = Command::new(&binary_abs)
            .current_dir(&data_abs)
            .env("PORT", self.port.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .process_group(0) // 独立进程组，免得本守护收 Ctrl-C 把它带走
            .spawn()
            .with_context(|| format!("启动 new-api 失败: {}", binary_abs.display()))?;

        let pid = child.id();
        std::fs::write(self.pid_file(), pid.to_string()).ok();
        info!(pid, port = self.port, log = %log_path.display(), "已拉起 new-api，等待健康…");

        for i in 1..=60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if self.is_healthy().await {
                info!(base = %self.base_url, secs = i, "new-api 健康");
                return Ok(());
            }
        }
        bail!(
            "new-api 启动后 60s 内未就绪，看看 {} 里的日志",
            log_path.display()
        );
    }

    /// 停止托管的 new-api（读 PID 文件发 SIGTERM）。
    pub fn stop(&self) -> Result<()> {
        let pf = self.pid_file();
        let pid: i32 = match std::fs::read_to_string(&pf) {
            Ok(s) => s.trim().parse().context("PID 文件内容非法")?,
            Err(_) => {
                warn!("没有 PID 文件，new-api 可能不是本工具起的，跳过");
                return Ok(());
            }
        };
        // SIGTERM（用 /bin/kill，省得引入 libc crate）
        let ok = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            info!(pid, "已向 new-api 发送 SIGTERM");
        } else {
            warn!(pid, "kill 失败，可能进程已退出");
        }
        std::fs::remove_file(&pf).ok();
        Ok(())
    }
}

/// F3：直写 `one-api.db` 把管理用户的内部额度提到 target（**只调大不调小**，
/// SQL 里 `quota < target` 守卫）。new-api 管理面没有改用户额度的 API
/// （EditWithTx 白名单不含 quota 且假成功），这是唯一路径。
///
/// rc.20 默认部署（无 Redis）下用户额度**每请求直查 DB**（model/user.go:961-969），
/// 运行中直写立即生效、无需重启——CLAUDE.md 旧结论（须重启）仅 Redis 模式成立。
/// busy_timeout 防 new-api 批量写锁（quota_data 每 5min 刷库）；失败不致命，下次启动再试。
pub fn bump_user_quota(
    m: &crate::config::ManageConfig,
    username: &str,
    target_quota: i64,
) -> anyhow::Result<()> {
    let db = std::path::Path::new(&m.data_dir).join("one-api.db");
    anyhow::ensure!(db.exists(), "SQLite 不存在：{}（new-api 还没首启？）", db.display());
    let conn = rusqlite::Connection::open(&db)
        .with_context(|| format!("打开 {} 失败", db.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(3))
        .context("设置 busy_timeout 失败")?;
    let updated = conn
        .execute(
            "UPDATE users SET quota = ?1 WHERE username = ?2 AND quota < ?1",
            rusqlite::params![target_quota, username],
        )
        .context("UPDATE users.quota 失败")?;
    anyhow::ensure!(
        updated > 0,
        "没有 username = {username} 且额度低于目标的用户行（用户名配错？）"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_manage() -> (crate::config::ManageConfig, Guard) {
        let dir = std::env::temp_dir().join(format!(
            "qt-boot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let m = crate::config::ManageConfig {
            version: String::new(),
            port: 0,
            data_dir: dir.to_string_lossy().into_owned(),
            repo: String::new(),
            root_user_quota_units: 0,
        };
        (m, Guard(dir))
    }

    /// 测试结束删临时目录
    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn seed_users(m: &crate::config::ManageConfig, quota: i64) {
        let db = std::path::Path::new(&m.data_dir).join("one-api.db");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, quota INTEGER)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (username, quota) VALUES ('root', ?1)",
            [quota],
        )
        .unwrap();
    }

    fn read_quota(m: &crate::config::ManageConfig) -> i64 {
        let db = std::path::Path::new(&m.data_dir).join("one-api.db");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.query_row("SELECT quota FROM users WHERE username='root'", [], |r| {
            r.get(0)
        })
        .unwrap()
    }

    #[test]
    fn 调额_只调大不调小_用户名必须匹配() {
        let (m, _g) = tmp_manage();
        seed_users(&m, 1000);

        // 低于目标 → 调大
        bump_user_quota(&m, "root", 1_000_000).unwrap();
        assert_eq!(read_quota(&m), 1_000_000);

        // 已高于目标 → 不动（ensure 失败，quota 保持）
        assert!(bump_user_quota(&m, "root", 500_000).is_err());
        assert_eq!(read_quota(&m), 1_000_000);

        // 用户名不匹配 → 报错
        assert!(bump_user_quota(&m, "nobody", 9_999_999).is_err());
        assert_eq!(read_quota(&m), 1_000_000);
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// 进程是否还活着（kill -0 不发信号只探测；pid ≤ 0 视为不存在）
fn process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// PID 是否真是 new-api（防 PID 复用误杀无辜进程——PR review #14）。
/// Linux 读 /proc/<pid>/cmdline（NUL 分隔，按字节读）验证二进制名；
/// 非 Linux 无 /proc → 无法核身，只信 PID 文件（本工具专用，风险剩人为伪造，可接受）。
fn process_is_newapi(pid: i32) -> bool {
    let cmdline = std::path::Path::new("/proc").join(pid.to_string()).join("cmdline");
    match std::fs::read(&cmdline) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).to_lowercase().contains("new-api"),
        Err(_) if !std::path::Path::new("/proc").exists() => true,
        // 进程残影 / 权限不可读——不冒险杀
        Err(_) => false,
    }
}

/// 等进程真正退出（SIGTERM 是异步的——旧进程 flush SQLite / 释放端口需要时间）。
/// 超时后放行（进程可能在不可中断状态，由用户处理）。
async fn wait_exit(pid: i32, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if !process_alive(pid) {
            info!(pid, "旧 new-api 进程已退出");
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    warn!(pid, "等待旧 new-api 退出超时（继续启动；若端口/SQLite 冲突请手动处理）");
}
