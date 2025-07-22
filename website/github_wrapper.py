import io

from github import Auth, Github
from github.Repository import Repository

from website.db import decrypt, dict_gen_one, get_db


class GithubWrapper:
    github: Github
    repo: Repository

    def __init__(self, token: str, repo: str, org: str):
        self.github = Github(auth=Auth.Token(token))
        if org:
            self.repo = self.github.get_user(org).get_repo(repo)
        else:
            self.repo = self.github.get_user().get_repo(repo)

    @classmethod
    def from_api_key(cls, api_key: str):
        database = get_db()
        c = database.cursor()
        c.execute("SELECT * FROM AirTableDatabases WHERE dbaseapikey=%s", (api_key,))
        existing = dict_gen_one(c)

        if existing is not None and "githubtoken" in existing:
            existing["githubtoken"] = decrypt(existing["githubtoken"])
            return cls(
                existing["githubtoken"],
                existing["githubrepo"],
                existing["githuborganization"],
            )

        return None

    def upload_file(self, file_path: str, content: io.BytesIO):
        content_file = None
        try:
            content_file = self.repo.get_contents(file_path)
            if isinstance(content_file, list):
                content_file = content_file[0]
        except Exception:
            pass

        if content_file is not None:
            self.repo.update_file(
                file_path, "Update file", content.getvalue(), content_file.sha
            )
        else:
            self.repo.create_file(file_path, "Create file", content.getvalue())
