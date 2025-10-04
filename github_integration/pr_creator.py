from github import Github

def create_ingestion_pr(repo_full_name: str, pr_branch: str, yaml_content: str, pr_title: str, pr_body: str):
    """
    Example: repo_full_name = "my-org/data-onboarding-repo"
    """
    token = "YOUR_GITHUB_TOKEN"
    gh = Github(token)
    repo = gh.get_repo(repo_full_name)

    # Create new branch from main
    source = repo.get_branch("main")
    repo.create_git_ref(ref=f"refs/heads/{pr_branch}", sha=source.commit.sha)

    # Create file in branch
    path = f"ingestion_configs/{pr_branch}.yaml"
    repo.create_file(path, "Add ingestion config via chatbot", yaml_content, branch=pr_branch)

    # Create pull request
    pr = repo.create_pull(title=pr_title, body=pr_body, head=pr_branch, base="main")
    return pr
