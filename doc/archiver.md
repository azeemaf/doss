How the archiver works
======================

Archiving is split into 3 phases:

* Selection
* Data copy
* Cleanup

The archiver requires two or more filesystems to be configured:

* The staging area (mandatory for all writable DOSS instances)
* The online master area (must support reads as well as writes)
* Optional secondary master area(s) (may be nearline)

Example
-------

Let's start out with an empty DOSS instance which has just had 3 files PUT into it and COMMITTED.

Thus in the staging area we have:

    /staging/nla.blob-1
    /staging/nla.blob-2
    /staging/nla.blob-3

And in the blobs database table we have:

blob_id | container_id | container offset
-----------------------------------------
1       | NULL         | NULL
2       | NULL         | NULL
3       | NULL         | NULL


Selection Phase
---------------

The archiver fetches a list of all committed blobs that have not yet been assigned to a container. That is 
all blobs where container_id == NULL. So blobs 1, 2, and 3. It then assigns these to containers.

1. Assign blob 1 to container 1.
2. Assign blob 2 to container 1.

At this point container 1 has exceeded the maximum container size threshold. So a new container 1 is marked
sealed and a new container id is generated.

3. Assign blob 3 to container 2.

This we end up in the blobs table:

blob_id | container_id | container offset
-----------------------------------------
1       | 1            | NULL
2       | 1            | NULL
3       | 2            | NULL

And in the containers table:

container_id | state
-----------------------
1            | SEALED
2            | OPEN
