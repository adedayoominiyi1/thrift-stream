Reactive-thrift
===============

A reactive implementation of the thrift protocol.

Work in progress.

Status:
* reactive decoding works
* not yet compatible with other thrift implementations
* no generated code available yet


Copyright (c) 2016 Erik van Oosten

Published under Apache Software License 2.0, see [LICENSE](LICENSE)



The current URI scheme for unicast is aeron:udp?local=<ip>(:<port>)?|remote=<ip>:<port>. However this naming is a little bit confusing as this only makes sense from a publication perspective. I.e. if you had a channel aeron:udp?local=host1|remote=host2:12345 it would implies that the publication is on host1 and the subscription is on host2. So when you are setting up a channel to be used by a subscription, the subscription is on the remote host, which is a little confusing.