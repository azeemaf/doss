DOSS 2 Native Read Interface
============================

The primary read interface to DOSS 2 will be a Java library that exposes Java
IO objects such as Channel (for random access) and InputStream (for read access).
We won't be exposing raw file paths so the application doesn't have to care
about them nor about the way we package small files into container files.

This also means down the track we can swap in different storage access protocols
(NFS, cluster filesystems, object storage protocols etc) merely by linking the
application against new version of the library.

While this is suitable for our in-house Java code how do we allow
access to collection items to third-party programs or those not written in Java?

Some programs including Wayback can use HTTP/1.1 access, but others like
IIPImage are heavily tied to random read access to a real file.

Option 1. C Library
-------------------

The most obvious solution is to write a DOSS client library in C similar to our
Java one.  This has the downside that it requires us to modify every
application in order to use it.  This will not always be possible or
straightforward, for example IIPSrv's file opening is actually done in Kakadu
rather than IIPSrv itself.

Option 2. FUSE
--------------

FUSE (Filesystem In Userspace) is a kernel module that allows filesystems to be
created by normal non-privileged programs in user space.  It's been a standard 
part of Linux for some time and was also included in Solaris 11. 

Essentialy we would write a "dossfs" FUSE backend which we could mount on
servers under a path like `/doss`.  This would enable you to read collection
items using any program without needing to worry about mapping paths, creating
temporary files or extracting records from within containers.  For example:

    grep foo /doss/12636362626
    convert /doss/2727226262 ~/newspap.jpg
    FILESYSTEM_PREFIX=/doss iipsrv

You might be wondering what happens when someone types `ls /doss`?  A FUSE
filesystem can choose which operations it supports and implementing `open` but
not supporting `readdir` works fine.

Option 3. NFS
-------------

[dCache] is the Java storage abstraction layer used by CERN and Fermilab.  The
way it solves this same problem is to implement a custom NFS server in Java.  The
code for this is open source and we could potentially use it in a similar way to
FUSE with the advantage it would be usable even from servers that don't support
FUSE.

[dCache]: http://www.dcache.org/
