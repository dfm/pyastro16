# -*- coding: utf-8 -*-

from __future__ import division, print_function

import time
import sqlite3
import requests

from settings import GITHUB_CLIENT_ID, GITHUB_CLIENT_SECRET

__all__ = ["download_issue_comments"]

BASE_URL = "https://api.github.com"
URL_FORMAT = BASE_URL + "/repos/{user}/{repo}/issues/comments"


def download_issue_comments(username, reponame, page=None):
    if page is None:
        page = 1
        with sqlite3.connect("issues.db") as conn:
            c = conn.cursor()
            try:
                c.execute("select last_page from repo_meta "
                          "where user_repo_name=?",
                          ("{0}/{1}".format(username, reponame),))

            except sqlite3.OperationalError:
                pass

            else:
                value = c.fetchone()
                if value is None:
                    page = 1
                else:
                    page = max(1, int(value[0]))

    session = requests.Session()
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=5))
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=5))

    # Timing:
    strt = time.time()

    # Base parameters:
    headers = {"User-Agent": "dfm"}
    params = dict(
        client_id=GITHUB_CLIENT_ID,
        client_secret=GITHUB_CLIENT_SECRET,
    )

    # Check the rate limit:
    url = BASE_URL + "/rate_limit"
    r = session.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
    reset = data["resources"]["core"]["reset"]
    if data["resources"]["core"]["remaining"] <= 0:
        print("Waiting for rate limit to reset...")
        reset = data["resources"]["core"]["reset"]
        time.sleep(reset - time.time() + 10)
    else:
        print("{0} requests remaining...".format(
            data["resources"]["core"]["remaining"]
        ))

    # Download some issue comments.
    url = URL_FORMAT.format(user=username, repo=reponame)
    params = dict(
        params,
        sort="created",
        direction="asc",
        per_page=100,
        page=page,
    )
    r = session.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()

    # If none, we're finished:
    if not len(data):
        return

    # Save the comments:
    with sqlite3.connect("issues.db") as conn:
        c = conn.cursor()
        c.execute("""
            create table if not exists issue_comments(
                comment_id integer primary key,
                comment_date text,
                username text,
                reponame text,
                issue_id integer,
                commenter text,
                comment text
            )
        """)

        for row in data:
            c.execute("""
                insert or ignore into issue_comments(
                    comment_id, comment_date, username, reponame, issue_id,
                    commenter, comment
                ) values (?,?,?,?,?,?,?)
            """, (
                row["id"], row["created_at"], username, reponame,
                int(row["issue_url"].split("/")[-1]), row["user"]["login"],
                row["body"],
            ))

        c.execute("""
            create table if not exists repo_meta(
                user_repo_name text primary key,
                last_page integer
            )
        """)
        c.execute("""
            insert or replace into repo_meta(
                user_repo_name, last_page
            ) values (?,?)
        """, ("{0}/{1}".format(username, reponame), page-1))

    print("Page {0}: added {1} comments in {2:.2f}s"
          .format(page, len(data), time.time()-strt))

    # Repeat:
    download_issue_comments(username, reponame, page=page+1)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: {0} username/reponame [page]".format(sys.argv[0]))
        sys.exit(1)

    username, reponame = sys.argv[1].split("/")
    download_issue_comments(username, reponame)
