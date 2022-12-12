class HttpException(Exception):
    def __init__(self, message: str, code=400, *args):
        super().__init__(message, *args)
        self.code = code
