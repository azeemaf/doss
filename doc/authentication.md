DOSS Authentication Proposal
============================

We want something that's easy to understand and configure, reasonably secure (ie not defeated just by sniffing a cleartext password off the wire) but also reasonably fast.

We also want some kind of identity for applications that we can hang access levels like "read-only" and "write access" off and which can be used for figuring out which crazy app has just queued 100,000 masters for staging.

We'll probably want some kind of command that shows all the connected clients and what they're up to and maybe some basic I/O stats for diagnosing performance problems.

So I'm going to try and work within those constraints and throw up a possible model.

Let's assume we have a DOSS server and some clients. Clients may be delivery systems, workflow systems and sysadmins using the CLI. Let's assume each client has some kind of key it uses to authenticate when accessing the server. The server has a list of valid keys and the set of access permissions each key grants.

Let's say there will be four permissions to start with:

* read: can read existing blobs
* write: can create new blobs
* stage: can stage offline blobs from tape
* admin: any special powers like managing permissions, killing connections etc

Okay where do the keys come from? Well one option is to have sysadmins manually generate and configure them when deploying the apps. But that sounds too much like work to me, I can see people taking shortcuts and giving a whole bunch of apps the same really weak key and putting them in some world readable config file that someone accidentally checks into version control. Not good.

Okay so what if each app generates its own random key on startup the first time it runs? Let's say the code to do that is part of the DOSS client library so client programmers don't even have to think about it, it just happens.

Great. But then there's the question of granting access. Do I need to find where the app stores its data, copy the generated key, paste it into the server's config, restart the server hoping I didn't make a syntax error and then restart the client to make it reconnect?

Again that sounds like work to me. How about this: as soon as the app generates the key it just connects to the server with it. The server lets it connect but doesn't let the client do or access anything because the server's never seen that key before.

We give the admin a command that lists the keys the server has seen and lets him set the access level for each:

    $ doss access
    # ACCESS  CLIENT          HOST      KEY FINGERPRINT
    1 read    doview          nailgun   ahpe7zohkexuuYee
    2 admin   bcollins (cli)  everest   laiSahTielar4ied
    3 (none)  dcm             renouf    meiy4Zu3ooghah6i
    4 (none)  books-ui        locutus   tahiwieGace9choo

    $ doss access +read,write,stage 3 

    $ doss access +read books-ui@locutus 

    $ doss access
    # ACCESS  CLIENT          HOST      KEY FINGERPRINT
    1 read    doview          spitfire  ahpe7zohkexuuYee
    2 admin   bcollins (cli)  everest   laiSahTielar4ied
    3 rws     dcm             renouf    meiy4Zu3ooghah6i
    4 read    books-ui        locutus   tahiwieGace9choo

Since the client app was allowed to connect there's no need to restart it, it'll just start working as soon as access is granted.

It should be possible to implement this scheme using SSL.  To avoid man in the middle attacks rather than using CAs, clients and servers 
just generate their own keys and you verify the key fingerprint matches via pre-existing secure channel. 