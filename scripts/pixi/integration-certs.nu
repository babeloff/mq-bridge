#!/usr/bin/env nu
# TLS fixtures the Docker-backed integration services expect. Three services
# need them; the generator takes one at a time.

let script = "tests/integration/scripts/gen_certs.sh"
if not ($script | path exists) {
    print --stderr $"missing ($script)"
    exit 1
}
^chmod +x $script
for svc in [mongodb kafka ibm-mq] {
    print $"::: certs for ($svc)"
    ^$"./($script)" $svc
}
print "certs generated for mongodb, kafka, ibm-mq"
