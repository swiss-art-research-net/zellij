import io
from github import Github, Auth
from github.Repository import Repository

class GithubWrapper:
    github: Github
    repo: Repository

    def __init__(self, token: str, repo: str):
        self.github = Github(auth=Auth.Token(token))
        self.repo = self.github.get_repo(repo)

    def upload_file(self, file_path: str, content: io.BytesIO):
        if self.repo.get_contents(file_path):
            file = self.repo.get_contents(file_path)

            if isinstance(file, list):
                file = file[0]

            self.repo.update_file(file_path, "Update file", content.getvalue(), file.sha)
        else:
            self.repo.create_file(file_path, "Create file", content.getvalue())
