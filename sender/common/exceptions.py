

class TaskError(Exception):
    pass


class AuthenticationError(TaskError):
    pass


class DownloadError(TaskError):
    pass


class SendMessageError(TaskError):
    pass