import sre_Duty

#!/usr/bin/env python3
"""
OTP P0 指引模块
"""

import sre_Duty

def get_otp_p0_guide():
    lines = []
    lines.append("- If there are 4 or more players encounter the issue, please first confirm whether it is currently ongoing.")
    lines.append("- Check logs first, based on the logs to refer the Scenario below")
    lines.append("")
    lines.append("<b>Scenario 1 (NO NEED OPEN P0 MEETING) : Status and Provider Status show SUCCESS</b>")
    lines.append('Kindly send screenshot to emergency group and say "OTP sent out success, Ask players to try and login again"')
    lines.append("")
    lines.append("<b>Scenario 2 (MUST OPEN P0 MEETING): Status and Provider Status show FAILED</b>")
    lines.append("<b>Kindly ask SRE to check the SMS server<b>")
    lines.append(sre_Duty.sretwoweek())
    lines.append("")
    lines.append("<b>Call the person below to join the meeting.</b>")
    lines.append("- Kindly call Jacob C 📞 09681199077")
    lines.append("- Kindly call Lim Lian Cheng first. If can't reach then call others.")
    lines.append("• Lim Lian Cheng 📞 60196549698")
    lines.append("• Qi Xiang 📞 ")
    lines.append("• Ho Ching 📞 60165010188")
    return "\n".join(lines)