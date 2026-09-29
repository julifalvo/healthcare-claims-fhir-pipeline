"""Record the README demo GIFs and screenshots from a running stack.

    docker compose up -d                                  # stack must be running and bootstrapped
    uv run --group demo playwright install chromium
    uv run --group demo python scripts/record_demo.py all

Requires ffmpeg on PATH. Outputs land in docs/assets/.
"""

import argparse
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "docs" / "assets"
WORK = ROOT / "scripts" / ".recordings"
AIRFLOW = "http://localhost:8080"
DASHBOARD = "http://localhost:8501"
GIF_WIDTH = 1200

# Playwright videos have no pointer; draw one so viewers can follow the interaction.
CURSOR_JS = """
window.addEventListener('DOMContentLoaded', () => {
  const dot = document.createElement('div');
  dot.style.cssText = 'position:fixed;z-index:2147483647;width:18px;height:18px;margin:-9px 0 0 -9px;'
    + 'border-radius:50%;background:rgba(42,120,214,.35);border:2px solid #2a78d6;'
    + 'pointer-events:none;transition:transform .08s;left:-40px;top:-40px';
  document.body.appendChild(dot);
  document.addEventListener('mousemove', e => { dot.style.left = e.clientX + 'px'; dot.style.top = e.clientY + 'px'; }, true);
  document.addEventListener('mousedown', () => dot.style.transform = 'scale(.7)', true);
  document.addEventListener('mouseup', () => dot.style.transform = 'scale(1)', true);
});
"""


def to_gif(source: str, output: Path, fps: int, input_args: list[str] | None = None) -> None:
    palette = (
        f"fps={fps},scale={GIF_WIDTH}:-1:flags=lanczos,split[a][b];"
        "[a]palettegen=max_colors=128:stats_mode=diff[p];"
        "[b][p]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle"
    )
    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel",
        "error",
        *(input_args or []),
        "-i",
        source,
        "-vf",
        palette,
        str(output),
    ]
    subprocess.run(cmd, check=True)
    print(f"wrote {output.relative_to(ROOT)} ({output.stat().st_size / 1e6:.1f} MB)")


def airflow_cli(*args: str) -> str:
    cmd = ["docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", *args]
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True).stdout


def run_state(run_id: str) -> str:
    for line in airflow_cli("dags", "list-runs", "claims_lakehouse", "-o", "plain").splitlines():
        if run_id in line:
            return line.split()[2]
    return "unknown"


def open_graph(page: Page, run_id: str) -> None:
    page.goto(f"{AIRFLOW}/dags/claims_lakehouse/runs/{run_id}", wait_until="networkidle")
    page.get_by_label("Show Graph", exact=False).first.click()
    page.wait_for_timeout(800)
    page.get_by_label("Expand all task groups").first.click()
    page.get_by_label("Collapse Details Panel").first.click()
    page.wait_for_timeout(1500)


def record_airflow(start: str, end: str, interval: float) -> None:
    run_id = f"demo_{datetime.now():%Y%m%dT%H%M%S}"
    frames = WORK / "airflow"
    shutil.rmtree(frames, ignore_errors=True)
    frames.mkdir(parents=True)
    conf = f'{{"start_date": "{start}", "end_date": "{end}"}}'
    airflow_cli("dags", "trigger", "claims_lakehouse", "--run-id", run_id, "--conf", conf)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720}, device_scale_factor=1.5)
        open_graph(page, run_id)
        n, state = 0, "queued"
        while state not in ("success", "failed"):
            page.screenshot(path=frames / f"frame_{n:04d}.png")
            n += 1
            time.sleep(interval)
            if n % 5 == 0:
                state = run_state(run_id)
        page.wait_for_timeout(3000)
        for _ in range(24):  # hold the final all-green graph for ~2s
            page.screenshot(path=frames / f"frame_{n:04d}.png")
            n += 1
        page.screenshot(path=ASSETS / "airflow-graph.png")
        page.get_by_label("Show Grid", exact=False).first.click()
        page.wait_for_timeout(2000)
        page.screenshot(path=ASSETS / "airflow-grid.png")
        page.goto(f"{AIRFLOW}/assets", wait_until="networkidle")
        page.wait_for_timeout(2000)
        page.screenshot(path=ASSETS / "airflow-assets.png")
        browser.close()
    print(f"run {run_id} finished: {state}, {n} frames")
    to_gif(
        str(frames / "frame_%04d.png"),
        ASSETS / "airflow-run.gif",
        fps=12,
        input_args=["-framerate", "12"],
    )


def record_dashboard() -> None:
    videos = WORK / "dashboard"
    shutil.rmtree(videos, ignore_errors=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            record_video_dir=str(videos),
            record_video_size={"width": 1440, "height": 900},
        )
        context.add_init_script(CURSOR_JS)
        page = context.new_page()
        page.goto(DASHBOARD, wait_until="networkidle")
        page.wait_for_selector("text=Denial rate by payer", timeout=60_000)
        page.wait_for_timeout(1500)

        def glide(x: int, y: int, steps: int = 25, pause: int = 600) -> None:
            page.mouse.move(x, y, steps=steps)
            page.wait_for_timeout(pause)

        glide(700, 300)
        for x in (300, 450, 600, 750):  # crosshair tooltip across the denial trend
            glide(x, 700, steps=15, pause=500)

        def open_tab(name: str, pause: int = 1800) -> None:
            tab = page.get_by_role("tab", name=name)
            box = tab.bounding_box()
            glide(int(box["x"] + box["width"] / 2), int(box["y"] + box["height"] / 2), pause=200)
            tab.click()
            page.wait_for_timeout(pause)

        open_tab("Denials")
        glide(560, 580, pause=1200)  # hover a denial reason bar
        page.mouse.wheel(0, 650)
        page.wait_for_timeout(1800)
        page.mouse.wheel(0, -650)
        page.wait_for_timeout(800)

        payer = page.locator('[data-testid="stMultiSelect"]').first
        box = payer.bounding_box()
        glide(int(box["x"] + 60), int(box["y"] + box["height"] / 2), pause=200)
        payer.click()
        page.wait_for_timeout(500)
        page.get_by_role("option", name="Harbor Health Advantage").click()
        page.keyboard.press("Escape")
        page.wait_for_timeout(2200)

        open_tab("A/R aging")
        open_tab("Readmissions")
        glide(600, 560, pause=1000)
        open_tab("Data quality", pause=2500)
        video = page.video.path()
        context.close()
        browser.close()
    to_gif(str(video), ASSETS / "dashboard.gif", fps=8)


def screenshots() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=1.5)
        page.goto(DASHBOARD, wait_until="networkidle")
        page.wait_for_selector("text=Denial rate by payer", timeout=60_000)
        page.wait_for_timeout(2000)
        page.screenshot(path=ASSETS / "dashboard-revenue.png")
        for tab, name in [
            ("Denials", "denials"),
            ("Readmissions", "readmissions"),
            ("Data quality", "data-quality"),
        ]:
            page.get_by_role("tab", name=tab).click()
            page.wait_for_timeout(2000)
            page.screenshot(path=ASSETS / f"dashboard-{name}.png", full_page=True)
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=["airflow", "dashboard", "screenshots", "all"])
    parser.add_argument("--start", default="2026-01-01")
    parser.add_argument("--end", default="2026-09-28")
    parser.add_argument(
        "--interval", type=float, default=2.0, help="seconds between Airflow frames"
    )
    args = parser.parse_args()
    ASSETS.mkdir(parents=True, exist_ok=True)
    if args.target in ("airflow", "all"):
        record_airflow(args.start, args.end, args.interval)
    if args.target in ("dashboard", "all"):
        record_dashboard()
    if args.target in ("screenshots", "all"):
        screenshots()


if __name__ == "__main__":
    main()
