use std::cmp::Ordering;
use std::env;
use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::daemon;

const BINARY_NAME: &str = "omc";
const REPOSITORY: &str = "tiejunhu/open-mcp-cli";

#[derive(Debug, PartialEq, Eq)]
struct RunningDaemon {
    socket_path: PathBuf,
    url: String,
}

pub(crate) async fn run_update_command() -> Result<(), Box<dyn Error>> {
    let latest_tag = resolve_latest_version()?;
    let current_version = env!("CARGO_PKG_VERSION");

    match compare_versions(current_version, &latest_tag)? {
        Ordering::Less => {}
        Ordering::Equal => {
            println!("omc {current_version} is already up to date");
            return Ok(());
        }
        Ordering::Greater => {
            println!(
                "omc {current_version} is newer than the latest published release {latest_tag}"
            );
            return Ok(());
        }
    }

    let executable_path = resolve_current_executable_path()?;
    let target = detect_target()?;
    let temp_dir = TempDir::new("open-mcp-cli-update")?;
    let asset_name = release_asset_name(&latest_tag, &target);
    let archive_path = temp_dir.path().join(&asset_name);
    let extracted_binary_path = temp_dir.path().join(BINARY_NAME);
    let download_url = release_asset_url(&latest_tag, &asset_name);

    println!("Downloading {asset_name}");
    download_file(&download_url, &archive_path)?;
    extract_archive(&archive_path, temp_dir.path())?;

    if !extracted_binary_path.is_file() {
        return Err(format!("release archive {asset_name} did not contain {BINARY_NAME}").into());
    }

    let daemons = discover_running_daemons().await?;
    if !daemons.is_empty() {
        println!("Stopping {} running daemon(s)", daemons.len());
        stop_daemons(&daemons).await?;
    }

    replace_executable(&extracted_binary_path, &executable_path)?;

    if !daemons.is_empty() {
        println!("Restarting {} daemon(s)", daemons.len());
        restart_daemons(&daemons)?;
    }

    println!("Updated omc from v{current_version} to {latest_tag}");
    Ok(())
}

fn resolve_latest_version() -> Result<String, Box<dyn Error>> {
    let latest_url = format!("{}/latest", releases_base());

    if command_exists("curl") {
        let output = run_command(
            "curl",
            [
                "-fsSL",
                "-o",
                "/dev/null",
                "-w",
                "%{url_effective}",
                &latest_url,
            ],
        )?;
        return parse_latest_release_tag(output.trim());
    }

    if command_exists("wget") {
        let output = run_command("wget", ["-S", "--spider", "--max-redirect=20", &latest_url])?;
        let final_url = output
            .lines()
            .filter_map(|line| line.trim().strip_prefix("Location: "))
            .last()
            .map(|value| value.trim_end_matches(" [following]").trim())
            .ok_or_else(|| {
                format!("failed to resolve latest release from {latest_url}: missing redirect")
            })?;
        return parse_latest_release_tag(final_url);
    }

    Err("update requires curl or wget".into())
}

fn parse_latest_release_tag(final_url: &str) -> Result<String, Box<dyn Error>> {
    let prefix = format!("{}/tag/", releases_base());
    final_url
        .strip_prefix(&prefix)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            format!("failed to resolve release tag from redirect target: {final_url}").into()
        })
}

fn releases_base() -> String {
    format!("https://github.com/{REPOSITORY}/releases")
}

fn detect_target() -> Result<&'static str, Box<dyn Error>> {
    let os = match env::consts::OS {
        "linux" => "unknown-linux-gnu",
        "macos" => "apple-darwin",
        other => return Err(format!("unsupported operating system: {other}").into()),
    };

    let arch = match env::consts::ARCH {
        "x86_64" => "x86_64",
        "aarch64" => "aarch64",
        other => return Err(format!("unsupported architecture: {other}").into()),
    };

    Ok(match (arch, os) {
        ("x86_64", "unknown-linux-gnu") => "x86_64-unknown-linux-gnu",
        ("x86_64", "apple-darwin") => "x86_64-apple-darwin",
        ("aarch64", "unknown-linux-gnu") => "aarch64-unknown-linux-gnu",
        ("aarch64", "apple-darwin") => "aarch64-apple-darwin",
        _ => unreachable!("supported arch/os combinations are exhaustive"),
    })
}

fn release_asset_name(version: &str, target: &str) -> String {
    format!("{BINARY_NAME}-{version}-{target}.tar.gz")
}

fn release_asset_url(version: &str, asset_name: &str) -> String {
    format!("{}/download/{version}/{asset_name}", releases_base())
}

fn download_file(url: &str, output: &Path) -> Result<(), Box<dyn Error>> {
    let output = output
        .to_str()
        .ok_or_else(|| format!("download path is not valid UTF-8: {}", output.display()))?;

    if command_exists("curl") {
        run_command("curl", ["-fsSL", "--retry", "3", "--output", output, url])?;
        return Ok(());
    }

    if command_exists("wget") {
        run_command("wget", ["-O", output, url])?;
        return Ok(());
    }

    Err("update requires curl or wget".into())
}

fn extract_archive(archive_path: &Path, output_dir: &Path) -> Result<(), Box<dyn Error>> {
    let archive = archive_path.to_string_lossy().into_owned();
    let output_dir = output_dir.to_string_lossy().into_owned();
    run_command("tar", ["-xzf", &archive, "-C", &output_dir])?;
    Ok(())
}

async fn discover_running_daemons() -> Result<Vec<RunningDaemon>, Box<dyn Error>> {
    let Some(directory) = default_daemon_directory()? else {
        return Ok(Vec::new());
    };

    let mut sockets = fs::read_dir(&directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| is_candidate_socket_path(path))
        .collect::<Vec<_>>();
    sockets.sort();

    let mut daemons = Vec::new();
    for socket_path in sockets {
        match daemon::request_status(None, Some(&socket_path)).await {
            Ok(status) => {
                let url = status.url.ok_or_else(|| {
                    format!(
                        "daemon at {} does not report its URL and cannot be restarted automatically",
                        socket_path.display()
                    )
                })?;
                daemons.push(RunningDaemon { socket_path, url });
            }
            Err(error) if is_daemon_not_running_error(error.as_ref()) => {}
            Err(error) => return Err(error),
        }
    }

    Ok(daemons)
}

fn default_daemon_directory() -> Result<Option<PathBuf>, Box<dyn Error>> {
    let socket_path = daemon::resolve_socket_path(None, None)?;
    match socket_path.parent() {
        Some(parent) if parent.exists() => Ok(Some(parent.to_path_buf())),
        Some(_) => Ok(None),
        None => Err("failed to determine daemon directory".into()),
    }
}

fn is_candidate_socket_path(path: &Path) -> bool {
    path.file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name.starts_with("daemon") && name.ends_with(".sock"))
}

fn is_daemon_not_running_error(error: &dyn Error) -> bool {
    error.to_string().starts_with("daemon is not running: ")
}

async fn stop_daemons(daemons: &[RunningDaemon]) -> Result<(), Box<dyn Error>> {
    for daemon_state in daemons {
        daemon::request_exit(None, Some(&daemon_state.socket_path)).await?;
    }

    Ok(())
}

fn restart_daemons(daemons: &[RunningDaemon]) -> Result<(), Box<dyn Error>> {
    let mut failures = Vec::new();

    for daemon_state in daemons {
        if let Err(error) =
            daemon::spawn_detached_daemon(&daemon_state.url, None, Some(&daemon_state.socket_path))
        {
            failures.push(format!("{} ({})", daemon_state.url, error));
        }
    }

    if failures.is_empty() {
        return Ok(());
    }

    Err(format!("failed to restart daemon(s): {}", failures.join("; ")).into())
}

fn replace_executable(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let destination = destination
        .parent()
        .ok_or_else(|| {
            format!(
                "failed to determine installation directory for {}",
                destination.display()
            )
        })?
        .join(destination.file_name().ok_or_else(|| {
            format!(
                "failed to determine executable file name for {}",
                destination.display()
            )
        })?);
    let staging_path = destination.with_file_name(format!(
        ".{}.update-{}.tmp",
        destination
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or(BINARY_NAME),
        process::id()
    ));

    fs::copy(source, &staging_path)?;
    fs::set_permissions(&staging_path, fs::metadata(source)?.permissions())?;

    if let Err(error) = fs::rename(&staging_path, &destination) {
        let _ = fs::remove_file(&staging_path);
        return Err(format!(
            "failed to replace executable {}: {error}",
            destination.display()
        )
        .into());
    }

    Ok(())
}

fn resolve_current_executable_path() -> Result<PathBuf, Box<dyn Error>> {
    let current_exe = env::current_exe()?;
    match fs::canonicalize(&current_exe) {
        Ok(path) => Ok(path),
        Err(_) => Ok(current_exe),
    }
}

fn compare_versions(current_version: &str, release_tag: &str) -> Result<Ordering, Box<dyn Error>> {
    let current = parse_numeric_version(current_version)?;
    let release = parse_numeric_version(release_tag)?;
    let width = current.len().max(release.len());

    for index in 0..width {
        let left = current.get(index).copied().unwrap_or(0);
        let right = release.get(index).copied().unwrap_or(0);
        match left.cmp(&right) {
            Ordering::Equal => continue,
            ordering => return Ok(ordering),
        }
    }

    Ok(Ordering::Equal)
}

fn parse_numeric_version(version: &str) -> Result<Vec<u64>, Box<dyn Error>> {
    let version = version.strip_prefix('v').unwrap_or(version);
    if version.is_empty() {
        return Err("version must not be empty".into());
    }

    let mut components = Vec::new();
    for part in version.split('.') {
        if part.is_empty() {
            return Err(format!("invalid version component in {version}").into());
        }
        components.push(part.parse()?);
    }

    Ok(components)
}

fn command_exists(command: &str) -> bool {
    Command::new(command)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn run_command<I, S>(program: &str, args: I) -> Result<String, Box<dyn Error>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program).args(args).output().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("`{program}` command not found")
        } else {
            format!("failed to run `{program}`: {error}")
        }
    })?;

    if output.status.success() {
        let stdout = String::from_utf8(output.stdout)?;
        let stderr = String::from_utf8(output.stderr)?;
        return Ok(format!("{stdout}{stderr}"));
    }

    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    let details = format!("{stdout}{stderr}");
    let details = details.trim();

    if details.is_empty() {
        Err(format!("`{program}` exited with status {}", output.status).into())
    } else {
        Err(format!("`{program}` failed: {details}").into())
    }
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Result<Self, Box<dyn Error>> {
        let base = env::temp_dir();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = base.join(format!("{prefix}-{}-{timestamp}", process::id()));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::path::Path;

    use super::{
        compare_versions, is_candidate_socket_path, parse_latest_release_tag,
        parse_numeric_version, release_asset_name,
    };

    #[test]
    fn parses_latest_release_tag_from_redirect_target() {
        let tag = parse_latest_release_tag(
            "https://github.com/tiejunhu/open-mcp-cli/releases/tag/v0.0.5",
        )
        .expect("expected release tag");

        assert_eq!(tag, "v0.0.5");
    }

    #[test]
    fn rejects_unexpected_latest_release_redirect_target() {
        let error = parse_latest_release_tag("https://example.com/releases/v0.0.5")
            .expect_err("expected invalid redirect");

        assert_eq!(
            error.to_string(),
            "failed to resolve release tag from redirect target: https://example.com/releases/v0.0.5"
        );
    }

    #[test]
    fn compares_versions_with_release_tags() {
        assert_eq!(
            compare_versions("0.0.5", "v0.0.6").expect("expected version comparison"),
            Ordering::Less
        );
        assert_eq!(
            compare_versions("0.0.6", "v0.0.6").expect("expected version comparison"),
            Ordering::Equal
        );
        assert_eq!(
            compare_versions("0.1.0", "v0.0.9").expect("expected version comparison"),
            Ordering::Greater
        );
    }

    #[test]
    fn parses_numeric_versions_without_prefix() {
        assert_eq!(
            parse_numeric_version("1.2.3").expect("expected numeric version"),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn parses_numeric_versions_with_v_prefix() {
        assert_eq!(
            parse_numeric_version("v1.2.3").expect("expected numeric version"),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn rejects_invalid_numeric_versions() {
        let error = parse_numeric_version("v1..3").expect_err("expected invalid numeric version");

        assert_eq!(error.to_string(), "invalid version component in 1..3");
    }

    #[test]
    fn builds_release_asset_name() {
        assert_eq!(
            release_asset_name("v0.0.5", "aarch64-apple-darwin"),
            "omc-v0.0.5-aarch64-apple-darwin.tar.gz"
        );
    }

    #[test]
    fn identifies_candidate_daemon_socket_paths() {
        assert!(is_candidate_socket_path(Path::new(
            "/tmp/daemon-example.com.sock"
        )));
        assert!(is_candidate_socket_path(Path::new("/tmp/daemon.sock")));
        assert!(!is_candidate_socket_path(Path::new(
            "/tmp/daemon-example.com.sock.ctl"
        )));
        assert!(!is_candidate_socket_path(Path::new("/tmp/tool-cache")));
    }
}
