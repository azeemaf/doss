DOSS Overview
=============

DOSS 2 is a greatly simplified filesystem abstraction intended to work around some of the preservation and management
issues encountered in DOSS 1 by:

* adding authentication and limited authorization
* performing integrity checking
* packing small objects into container files for more efficient use of some storage types
* allowing objects to migrated between different storage devices without applications caring

Unlike a traditional filesystem, DOSS has no notion of directories, filenames or permissions.  Objects are identified
solely by a 64-bit integer id.  Once written an object cannot be modified.  Making sense of the collection via metadata
and providing access controls is expected to be done by a higher layer.

DOSS consists of the following components:

* DOSS server daemon
* DOSS client library
* doss-nfsd native gateway

Client API
----------

DOSS provides a small Java API to clients.  The primary interface is the "blob store" which allows new objects to 
be deposited in exchange for an id and existing objects to be retrieved by their id.  The API provides limited information 
about stored objects such as their size, digest and when they were created but is intentionally not for general metadata.

As the network protocol is still under development applications use a LocalBlobStore on top of a local filesystem and H2
database.  Once finished however RemoteBlobStore will take its place.

Server
------

A DOSS BlobStore consists of multiple storage areas.  In production we expect to use two: "staging" and "preservation".  Areas area in turn backed by one or more filesystems which each hold a complete identical
copy of the area's objects.  Objects are initially written to the staging area, a regular disk filesystem.  Once a sufficient number of objects
are written they are packed together into a container (currently tar files) and moved to the preservation area
for writing to archival tape and mass delivery storage.

In addition to managing packing the staging area gives us a time window where objects can be removed (failed transactions, 
mistakes) before they're permanently archived.  It also means we can put the preservation area into a read-only 
mode during hardware maintenance without impacting applications at all.

Applications access objects in any storage area and even in the middle of a migration without being aware of where
they currently physically reside.

DOSS keeps track of the current status and locations of objects in its own SQL database.  This database is merely
an index and can always be rebuilt from the contents of the filesystems in the event it is lost or damaged.

doss-nfsd native gateway
------------------------

While the majority of the DLIR codebase is written in Java we're using one notable C++ application, the IIP image
service.  Doss-nfsd provides read-only access to a DOSS blob store for non-Java applications.  It translates between
NFS4 and the DOSS protocol.  This allows you to mount a blob store as a local filesystem an access objects via
simple paths like /blobstore/2712.jp2.

doss-nfsd itself is quite small and delegates most of the hard work to jpnfs, an open source Java
NFS server that was developed for dCache a grid storage system for high-energy physics research data.

Authentication
--------------

(Work in progress.)

On first startup each client generates a keypair and then attempts to connect to the server.  After first connect an
administrator will need to authorise the client by setting an access level (such as read-only, write access, staging
or admin).  This mechanism controls access to the Blob Store not to individual objects.  Individual object access
policies will necessarily need to be handled in a higher layer as it's dependent on detailed knowledge of the 
collection.  See [authentication.md](authentication.md) for more details.

The client library handles all the details of key generation and authentication and there is no need for any 
configuration by client application code.

Integrity checking
------------------

(Not implemented yet.)

When an object is written to the blob store, DOSS calculates a digest of its contents.  This digest can be retrieved
via the client API so an application can verify the transfer was successful.

After a container has been packed its contents are verified and a digest for the entire container is calculated after
which it is considered sealed and becomes a candidate for archiving.  As containers are moved between storage areas
this digest is used to ensure consistency.

In addition the DOSS server can be configured to do periodic background scanning of storage media to verify digests
of containers that are online.  When a discrepancy is found system administrators will be alerted and can take corrective
action such as retrieving an alternate copy from the offline backup location.
