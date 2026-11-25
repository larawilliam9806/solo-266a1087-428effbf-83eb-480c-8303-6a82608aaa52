"""应用层错误类型。"""


class ValidationError(Exception):
    """请求数据不合法，对应 HTTP 400。"""

    status = 400

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NotFoundError(Exception):
    """目标记录不存在，对应 HTTP 404。"""

    status = 404

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class LockUnavailableError(Exception):
    """SQLite 写锁在限定时间内一直被占用，对应 HTTP 503，可安全重试。

    抛出该错误时尚未开启写事务，因此不会入账，也不会改变合并摘要；
    调用方稍后原样重试同一请求即可。
    """

    status = 503
    retryable = True

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
