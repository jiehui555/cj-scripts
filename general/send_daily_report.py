from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
import shutil
import smtplib
from typing import Any, List, Optional
import zipfile
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, ViewportSize, Page, Locator
from PIL import Image


def config(key: str, default: Any = None) -> Any:
    """获取配置"""
    data = {
        "TEMP_DIR": os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp"),
        "CJPLUS_URL": os.getenv("CJPLUS_URL"),
        "CJPLUS_USERNAME": os.getenv("CJPLUS_USERNAME"),
        "CJPLUS_PASSWORD": os.getenv("CJPLUS_PASSWORD"),
        "CJPLUS_REPORTS": [
            {"name": "今日新单报表", "page": 208},
            {"name": "延期出货明细表", "page": 220},
            {"name": "宏智出货报表", "page": 210, "has_tail": False},
            {"name": "技果出货报表", "page": 207, "has_tail": False},
            {"name": "迅成出货报表", "page": 206, "has_tail": False},
            {"name": "金安出货报表", "page": 212, "has_tail": False},
            {"name": "长嘉出货报表", "page": 205, "has_tail": True},
        ],
        "SMTP_HOST": os.getenv("SMTP_HOST"),
        "SMTP_PORT": os.getenv("SMTP_PORT"),
        "SMTP_FROM": os.getenv("SMTP_FROM"),
        "SMTP_PASS": os.getenv("SMTP_PASS"),
        "SMTP_TO": os.getenv("SMTP_TO"),
    }
    return data.get(key, default)


def screenshot():
    """截取图片"""
    img_paths = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        logging.info("启动浏览器")

        viewport = ViewportSize(width=1920, height=1080)
        context = browser.new_context(viewport=viewport)
        logging.info("创建上下文")

        page = context.new_page()
        logging.info("创建新页面")

        # 登录后台
        page.goto(config("CJPLUS_URL"), wait_until="networkidle", timeout=30_000)
        page.wait_for_selector('input[name="user"]', timeout=5_000)
        logging.info("已加载登录页面")

        page.fill('input[name="user"]', config("CJPLUS_USERNAME"))
        page.fill('input[name="pass"]', config("CJPLUS_PASSWORD"))
        page.click('input[type="submit"]')
        page.wait_for_load_state("networkidle", timeout=30_000)
        logging.info("已成功登录后台")

        # 处理每个报表
        for report in config("CJPLUS_REPORTS"):
            url = f"{config('CJPLUS_URL')}/utl/{report['page']}/{report['page']}.php"
            logging.info(f'处理报表：{report["name"]} - {url}')

            if report["name"] == "今日新单报表":
                img_path = __screenshot_new_order_report(page, url)
            elif report["name"] == "延期出货明细表":
                img_path = __screenshot_delay_shipment_report(page, url)
            else:
                img_path = __screenshot_company_shipment_report(
                    page, url, report["name"], report.get("has_tail", False)
                )

            img_paths.append(img_path)
            logging.info(f"已完成截图：{img_path}")

    return img_paths


def __screenshot_new_order_report(page: Page, url: str) -> str:
    """截取「今日新单报表」"""
    page.goto(url, wait_until="networkidle", timeout=60_000)
    page.wait_for_selector("#table", state="visible", timeout=5_000)
    logging.info("已加载数据表格页")

    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    img_path = os.path.join(config("TEMP_DIR"), f"今日新单报表_{today}.png")
    page.locator("#table").screenshot(path=img_path)
    logging.info(f"已截取数据表格页：{img_path}")

    return img_path


def __screenshot_delay_shipment_report(page: Page, url: str) -> str:
    """截取「延期出货明细表」"""
    page.goto(url, wait_until="networkidle", timeout=30_000)
    page.wait_for_selector("#table", state="visible", timeout=5_000)
    logging.info("已加载数据表格页")

    page.locator("#header").evaluate("el => el.style.display = 'none'")
    logging.info("已隐藏顶部表单")

    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    img_path = os.path.join(config("TEMP_DIR"), f"延期出货明细表_{today}.png")

    page.locator("#table").screenshot(path=img_path)
    logging.info(f"已截取数据表格页：{img_path}")

    return img_path


def __screenshot_company_shipment_report(
    page: Page, url: str, report: str, has_tail: bool
) -> str:
    """截取「公司出货报表」"""

    img_paths = []  # 局部截图
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    TEMP_DIR = config("TEMP_DIR")

    # 加载数据表格页
    page.goto(url, wait_until="networkidle", timeout=30_000)
    page.wait_for_selector("table", state="visible", timeout=5_000)
    logging.info("已加载数据表格页")

    # 截取表头
    img_path = os.path.join(TEMP_DIR, f"{report}_局部截图-表头.png")
    page.locator("thead").screenshot(path=img_path)
    img_paths.append(img_path)
    logging.info(f"已截取表头：{img_path}")

    # 截取延期出货
    img_path = os.path.join(TEMP_DIR, f"{report}_局部截图-延期出货.png")
    page.locator('tbody[data-type="延期出货"]').screenshot(path=img_path)
    img_paths.append(img_path)
    logging.info(f"已截取延期出货：{img_path}")

    # 截取货尾
    if has_tail:
        img_path = os.path.join(TEMP_DIR, f"{report}_局部截图-货尾.png")
        page.locator('tbody[data-type="货尾"]').screenshot(path=img_path)
        img_paths.append(img_path)
        logging.info(f"已截取货尾：{img_path}")

    # 截取月份数据
    for i in range(now.month, min(now.month + 3, 13)):
        css_locator = f'tbody[data-type="{i} 月"]'
        if page.locator(css_locator).count() == 0:
            __append_blank_month_tbody(page.locator("table"), i)
            logging.info(f"添加空白的 {i} 月数据")
        img_path = os.path.join(TEMP_DIR, f"{report}_局部截图-{i} 月.png")
        page.locator(css_locator).screenshot(path=img_path)
        img_paths.append(img_path)
        logging.info(f"已截取 {i} 月数据：{img_path}")

    # 处理跨年数据
    if now.month > 10:
        logging.info(f"当前月份 {now.month} > 10，需截取 {now.year + 1} 年 1、2 月数据")
        page.click('input[name="年份"]', timeout=10_000)
        page.click(f'li[lay-ym="{now.year + 1}"]', timeout=10_000)
        page.wait_for_load_state("networkidle", timeout=30_000)
        page.wait_for_selector("table", state="visible", timeout=10_000)

        for i in range(1, 3):
            css_locator = f'tbody[data-type="{i} 月"]'
            if page.locator(css_locator).count() == 0:
                __append_blank_month_tbody(page.locator("table"), i)
                logging.info(f"添加空白的 {i} 月数据")
            img_path = os.path.join(TEMP_DIR, f"{report}_局部截图-{i} 月.png")
            page.locator(css_locator).screenshot(path=img_path)
            img_paths.append(img_path)

    # 确定合并顺序
    if now.month < 11:
        months = range(now.month + 2, now.month - 1, -1)
    elif now.month == 11:
        months = [1, 12, 11]
    else:
        months = [2, 1, 12]

    merge_order = [
        os.path.join(TEMP_DIR, f"{report}_局部截图-表头.png"),
        os.path.join(TEMP_DIR, f"{report}_局部截图-延期出货.png"),
    ]
    if has_tail:
        merge_order.append(os.path.join(TEMP_DIR, f"{report}_局部截图-货尾.png"))
    merge_order += [
        os.path.join(TEMP_DIR, f"{report}_局部截图-{m} 月.png") for m in months
    ]
    logging.info(f"需要合并的图片：{merge_order}")

    # 合并图片
    save_name = f"{report}_{now.strftime('%Y-%m-%d')}"
    full_img_path = __merge_images(merge_order, save_name, output_dir=TEMP_DIR)
    logging.info(f"图片合并完成，保存路径: {full_img_path}")

    return full_img_path


def __append_blank_month_tbody(locator: Locator, thead_month: str | int):
    """添加空白的月份数据"""
    tbody = f"""
            <tbody data-type="{thead_month} 月">
                <tr class="bg-blue">
                    <th rowspan="5" class="border-left">{thead_month} 月</th>
                    <th>事业部</th>
                    <th class="border-right">订单</th>
                    <th>预出数</th>
                    <th>预收入（万）</th>
                    <th class="border-right text-red">预利润（万）</th>
                    <th>已出数</th>
                    <th>实收金额（万）</th>
                    <th class="border-right text-red">实收利润（万）</th>
                </tr>
                <tr><td class="bg-blue">TV</td><td class="border-right">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td></tr>
                <tr><td class="bg-blue">SX</td><td class="border-right">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td></tr>
                <tr><td class="bg-blue">MT</td><td class="border-right">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td></tr>
                <tr><td class="bg-blue">小结</td><td class="border-right">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td><td>-</td><td>-</td><td class="border-right text-red">-</td></tr>
            </tbody>
        """

    locator.evaluate(
        """
            (element, html) => {
                element.insertAdjacentHTML('beforeend', html);
            }
        """,
        arg=tbody,
    )


def __merge_images(
    img_paths: List[str],
    output_name: str,
    background: tuple = (255, 255, 255),
    output_dir: Optional[str] = None,
) -> str:
    """合并多张图片"""
    # 打开所有图片
    images = [Image.open(img_path) for img_path in img_paths]

    # 获取图片尺寸
    widths = [img.size[0] for img in images]
    heights = [img.size[1] for img in images]

    # 计算最大宽度和总高度
    max_width = max(widths)
    total_height = sum(heights)

    # 创建新空白图片
    new_img = Image.new("RGB", (max_width, total_height), background)

    # 粘贴图片到新图片
    y_offset = 0
    for img in images:
        x_offset = (max_width - img.width) // 2
        new_img.paste(img, (x_offset, y_offset))
        y_offset += img.height

    # 保存新图片
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        save_path = os.path.join(output_dir, f"{output_name}.png")
    else:
        save_path = f"{output_name}.png"

    new_img.save(save_path, quality=95)

    return save_path


def send_report_email(img_paths: List[str]) -> None:
    """发送报表邮件"""
    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")

    # 打包图片
    zip_path = os.path.join(config("TEMP_DIR"), f"每日截图-打包-{today}.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for result_img_path in img_paths:
            if os.path.exists(result_img_path):
                filename = os.path.basename(result_img_path)
                zipf.write(result_img_path, arcname=filename)
            else:
                logging.warning(f"文件不存在，跳过 {result_img_path}")
    logging.info(f"已打包图片：{zip_path}")

    # 发送邮件
    message = MIMEMultipart()
    message["Subject"] = f"每日截图 - {today}"
    message["From"] = config("SMTP_FROM")
    message["To"] = config("SMTP_TO")

    body = f"""
        <p>附件是今天的所有报表截图打包（{today}）</p>
        <p>如有问题请检查运行状态</p>
    """
    message.attach(MIMEText(body, "html"))
    if os.path.exists(zip_path):
        with open(zip_path, "rb") as file:
            filename = os.path.basename(zip_path)
            message.attach(MIMEApplication(file.read(), Name=filename))

    with smtplib.SMTP_SSL(config("SMTP_HOST"), config("SMTP_PORT")) as server:
        server.login(config("SMTP_FROM"), config("SMTP_PASS"))
        server.sendmail(config("SMTP_FROM"), [config("SMTP_TO")], message.as_string())
        server.quit()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    load_dotenv()
    logging.info("开始执行")

    # 重置临时目录
    TEMP_DIR = f"{config('TEMP_DIR')}"
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR, exist_ok=True)
    logging.info(f"已清理并创建临时目录: {TEMP_DIR}")

    # 截取图片
    images = screenshot()
    logging.info(f"图片截取完成，共{len(images)}张")

    # 打包并发送邮件
    send_report_email(images)
    logging.info("已发送邮件")

    logging.info("执行完毕")
