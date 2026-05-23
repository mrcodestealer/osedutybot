import pyotp
import qrcode
from io import BytesIO
from PIL import Image
import sys

# ========== 配置部分 ==========
# 示例密钥（与你的脚本一致）
TOTP_SECRET = "MNYG63JQGEYTMOJTHE4DMMBTGQYDIOI"
# 账号名称（自定义，显示在 Authenticator 中）
ACCOUNT_NAME = "cpomduty"
# 颁发者（可选，分组用）
ISSUER = "SMS-CP"

# ========== 1. 从密钥生成二维码 ==========
def generate_qr_from_secret(secret, account_name, issuer=None, output_file="sms_qr.png"):
    """
    根据 TOTP 密钥生成二维码，保存为图片文件
    """
    # 构建 otpauth URI
    if issuer:
        uri = pyotp.totp.TOTP(secret).provisioning_uri(
            name=account_name, issuer_name=issuer
        )
    else:
        uri = pyotp.totp.TOTP(secret).provisioning_uri(name=account_name)

    print(f"🔑 密钥 (Secret): {secret}")
    print(f"🔗 OTP Auth URI: {uri}")

    # 生成二维码
    qr = qrcode.QRCode(box_size=10, border=4)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    img.save(output_file)
    print(f"✅ 二维码已保存为: {output_file}")
    return output_file

# ========== 2. 从二维码图片解析出密钥 ==========
def parse_secret_from_qr_image(image_path):
    """
    从二维码图片文件中解析出 TOTP 密钥
    需要安装 opencv-python 和 pyzbar
    """
    try:
        from pyzbar.pyzbar import decode
        from PIL import Image
    except ImportError:
        print("❌ 请安装 pyzbar 和 PIL: pip install pyzbar pillow")
        return None

    img = Image.open(image_path)
    decoded = decode(img)
    if not decoded:
        print("❌ 未在图片中检测到二维码")
        return None

    data = decoded[0].data.decode('utf-8')
    print(f"📄 二维码内容: {data}")

    # 解析 otpauth URI 获取 secret
    if data.startswith("otpauth://"):
        # 简单正则或字符串分割
        import re
        match = re.search(r'secret=([A-Z2-7]+)', data, re.IGNORECASE)
        if match:
            secret = match.group(1)
            print(f"🔑 解析出的密钥: {secret}")
            return secret
        else:
            print("❌ URI 中未找到 secret 参数")
            return None
    else:
        print("❌ 二维码内容不是有效的 OTP Auth URI")
        return None

# ========== 主程序示例 ==========
if __name__ == "__main__":
    # 1. 生成二维码（从已有密钥）
    print("=== 生成二维码 ===")
    qr_file = generate_qr_from_secret(
        secret=TOTP_SECRET,
        account_name=ACCOUNT_NAME,
        issuer=ISSUER,
        output_file="sms_qr.png"
    )

    # 2. 演示从刚才生成的二维码解析回密钥（可选）
    print("\n=== 从二维码解析密钥 ===")
    parsed_secret = parse_secret_from_qr_image(qr_file)
    if parsed_secret:
        assert parsed_secret == TOTP_SECRET, "解析结果与原密钥不一致！"
        print("✅ 解析验证成功，密钥匹配")
    else:
        print("⚠️ 解析失败，请检查依赖或图片质量")

    # 你也可以直接使用密钥字符串，不需要二维码
    print(f"\n📌 直接使用密钥: {TOTP_SECRET}")
    print("将此密钥手动输入到 Google Authenticator 或 Authy 等应用中即可。")