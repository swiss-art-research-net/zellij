import io
from github import Github, Auth
from github.Repository import Repository


class GithubWrapper:
    github: Github
    repo: Repository

    def __init__(self, token: str, repo: str, org: str):
        self.github = Github(auth=Auth.Token(token))
        if org:
            self.repo = self.github.get_user(org).get_repo(repo)
        else:
            self.repo = self.github.get_user().get_repo(repo)

    def upload_file(self, file_path: str, content: io.BytesIO):
        content_file = None
        try:
            content_file = self.repo.get_contents(file_path)
            if isinstance(content_file, list):
                content_file = content_file[0]
        except Exception as e:
            pass

        if content_file is not None:
            self.repo.update_file(
                file_path, "Update file", content.getvalue(), content_file.sha
            )
        else:
            self.repo.create_file(file_path, "Create file", content.getvalue())
