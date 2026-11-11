class ApiError(Exception):
    # returned to the client as {"error": message, "code": code}; clients should switch on code, not message
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
