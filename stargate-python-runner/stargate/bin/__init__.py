from datetime import datetime

now = datetime.now()
__version__ = now.strftime("0.0.0.%Y%m%d")
