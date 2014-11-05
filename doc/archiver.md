How the archiver works
======================

Archiving is split into 3 phases corresponding to a change of state tracked in the containers database table:

* Selection (OPEN -> SEALED)
* Data copy (SEALED -> WRITTEN)
* Cleanup   (WRITEN -> ARCHIVED)

The archiver will normally run each phase one after the other.

The archiver requires two or more filesystems to be configured:

* The staging area (mandatory for all writable DOSS instances)
* The online master area (must support reads as well as writes)
* Optional secondary master area(s) (may be nearline)

Example
-------

Let's start out with an empty DOSS instance which has just had 3 files PUT into it and COMMITTED. It has two
empty master filesystems called "display" and "nearline":

    /display/data
    /display/incoming
    /nearline/data
    /nearline/incoming   (archive -n)

Thus in the staging area we have:

    /staging/nla.blob-1
    /staging/nla.blob-2
    /staging/nla.blob-3

And in the blobs database table we have:

blob_id | container_id | container offset
--------|--------------|-----------------
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
--------|--------------|-----------------
1       | 1            | NULL
2       | 1            | NULL
3       | 2            | NULL

And in the containers table:

container_id | state
-------------|---------
1            | SEALED
2            | OPEN

Optional Seal All Phase
-----------------------

At this point if asked to with an option the archiver will seal any remaining OPEN containers to ensure all of a days work is archived.

container_id | state
-------------|---------
1            | SEALED
2            | SEALED


Data Copy Phase
---------------

The archiver fetches the list of sealed containers. For each container it does the following steps.

1. Creates a new tar file in the incoming directories of each master filesystem.  Truncating it if already present.

    /display/incoming/nla.doss-1.tar
    /nearline/incoming/nla.doss-1.tar

2. Reads each blob that was assigned to this container from staging and writes it to both tar files.
3. Saves the offset into the tar file in the database.
3. Calls fsync() on both tar files.
4. Reopens both tar files for reading.
5. Reads each record and verifies the data is correct.
6. Calculates a digest for both tar files and compares them to each other to make sure both are identical.
7. Moves the tar files to their final destination:

    /display/data/nla.doss-1.tar
    /nearline/data/nla.doss-1.tar
8. Updates the database and sets the container state to "WRITTEN".

After the data copy phase we have this in the database:

blob_id | container_id | container offset
--------|--------------|-----------------
1       | 1            | 0
2       | 1            | 2726262
3       | 2            | 0

container_id | state
-------------|---------
1            | WRITTEN
2            | WRITTEN

Once the container state becomes "WRITTEN", normal blobstore read operations are directed to the display filesystem rather than staging filesystem.

Cleaned phase
-------------

The cleanup phase deletes the original blob files from the staging filesystem. It updates the container state "ARCHIVED" to make the cleanup as complete.

container_id | state
-------------|---------
1            | ARCHIVED
2            | ARCHIVED

Notes on error handling
-----------------------

Each phase is completely independent can be run multiple times without harm.  If a phase encounters an I/O error it will close any open files and exit throwing an exception.  The archiver can be re-executed once the underlying filesystems are fixed.  Some steps will then have been run twice for the one container, but that's ok, they will just overwrite any incomplete work from when the error occurred.
