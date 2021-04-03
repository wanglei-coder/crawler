from dataclasses import dataclass

import yagmail


@dataclass(unsafe_hash=True)
class Info:
    user: str = "coderwanglei"
    sender: str = "coderwanglei@163.com"
    receivers: str = "1393918981@qq.com"
    passwd: str = "CHVBVPLCLTKBOBKM"
    sever: str = "smtp.163.com"


class SendEmail:
    def __init__(self, info: Info):
        self.info = info

    def send_email(self, subject, contents, filename):
        with yagmail.SMTP(user=self.info.sender,
                          password=self.info.passwd,
                          host=self.info.sever) as s:
            s.send(to=self.info.receivers, subject=subject, contents=contents, attachments=filename)


if __name__ == '__main__':
    c = Info()
    SendEmail(c).send_email("test", "context", "/home/mamba/lyz/archieve/bj.csv")
