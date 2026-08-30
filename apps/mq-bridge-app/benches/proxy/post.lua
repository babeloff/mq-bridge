wrk.method = "POST"
wrk.body   = string.rep("x", 200)
wrk.headers["Content-Type"] = "application/octet-stream"
