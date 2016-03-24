#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division, print_function

import time
import sqlite3
import requests

from settings import GITHUB_CLIENT_ID, GITHUB_CLIENT_SECRET

__all__ = ["download_issue_comments"]

BASE_URL = "https://api.github.com"
ISSUES_FORMAT = BASE_URL + "/repos/{user}/{repo}/issues"
COMMENTS_FORMAT = BASE_URL + "/repos/{user}/{repo}/issues/comments"


def get_connection():
    return sqlite3.connect("issues.db")


def create_tables():
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("""
            create table if not exists issue_comments(
                comment_id integer,
                is_base integer,
                comment_date text,
                username text,
                reponame text,
                issue_id integer,
                commenter text,
                comment text,
                unique (comment_id, is_base, comment_date)
            )
        """)
        c.execute("""
            create table if not exists repo_meta(
                user_repo_name text primary key,
                last_page integer
            )
        """)


def download_issue_comments(username, reponame, base=False, page=None):
    if base:
        user_repo_name = "{0}/{1}/issues".format(username, reponame)
        url = ISSUES_FORMAT.format(user=username, repo=reponame)
    else:
        user_repo_name = "{0}/{1}".format(username, reponame)
        url = COMMENTS_FORMAT.format(user=username, repo=reponame)

    if page is None:
        page = 1
        with get_connection() as conn:
            c = conn.cursor()
            try:
                c.execute("select last_page from repo_meta "
                          "where user_repo_name=?",
                          (user_repo_name,))

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

    # Download some issue comments.
    params = dict(
        params,
        sort="created",
        direction="asc",
        per_page=100,
        page=page,
    )
    params["state"] = "all"
    r = session.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
    rhdr = r.headers

    # If none, we're finished:
    if not len(data):
        return

    # Save the comments:
    with sqlite3.connect("issues.db") as conn:
        c = conn.cursor()
        for row in data:
            if base:
                issue_id = int(row["number"])
            else:
                issue_id = int(row["issue_url"].split("/")[-1])
            c.execute("""
                insert or replace into issue_comments(
                    comment_id, is_base, comment_date, username, reponame,
                    issue_id, commenter, comment
                ) values (?,?,?,?,?,?,?,?)
            """, (
                row["id"], False, row["created_at"], username, reponame,
                issue_id, row["user"]["login"], row["body"],
            ))
            if base:
                c.execute("""
                    insert or replace into issue_comments(
                        comment_id, is_base, comment_date, username, reponame,
                        issue_id, commenter, comment
                    ) values (?,?,?,?,?,?,?,?)
                """, (
                    row["id"], True, row["created_at"], username, reponame,
                    issue_id, row["user"]["login"], row["title"],
                ))
        c.execute("""
            insert or replace into repo_meta(
                user_repo_name, last_page
            ) values (?,?)
        """, (user_repo_name, page))

    print("{0} page {1}: added {2} comments in {3:.2f}s"
          .format("Issues" if base else "Comments", page, len(data),
                  time.time()-strt))

    # Wait for rate limit:
    reset = rhdr["X-RateLimit-Reset"]
    remaining = int(rhdr["X-RateLimit-Remaining"])
    if remaining <= 0:
        print("Waiting for rate limit to reset...")
        time.sleep(max(0, reset - time.time() + 10))
    else:
        print("{0} requests remaining...".format(remaining))

    # Repeat:
    download_issue_comments(username, reponame, base=base, page=page+1)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: {0} username/reponame".format(sys.argv[0]))
        sys.exit(1)

    create_tables()

    username, reponame = sys.argv[1].split("/")
    download_issue_comments(username, reponame, base=True)
    download_issue_comments(username, reponame)
