#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division, print_function

import os
import sqlite3

__all__ = ["dump_repo_issues"]


def get_connection():
    return sqlite3.connect("issues.db")


def dump_repo_issues(username, reponame):
    txt = ""
    with get_connection() as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            select * from issue_comments where username=? and reponame=?
            order by issue_id, comment_date, is_base desc
        """, (username, reponame))
        for row in c:
            if row["is_base"]:
                txt += "</issue>\n<issue>\n"
            if row["comment"] is None:
                continue
            txt += "<author>" + row["commenter"] + "</author>\n"
            txt += row["comment"] + "\n"
    txt = txt[len("</issue>\n"):] + "</issue>\n"
    fn = os.path.join("dumps", "{0}-{1}.txt".format(username, reponame))
    with open(fn, "w") as f:
        f.write(txt)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: {0} username/reponame".format(sys.argv[0]))
        sys.exit(1)

    os.makedirs("dumps", exist_ok=True)
    username, reponame = sys.argv[1].split("/")
    dump_repo_issues(username, reponame)
