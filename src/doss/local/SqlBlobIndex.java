package doss.local;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;

import doss.NoSuchBlobException;

/**
 * BlobIndex backed by a SQL database using jDBI.
 * 
 * This class does not own any jDBI resources and is not responsible for any
 * jDBI resource management.
 */
public class SqlBlobIndex implements BlobIndex {

    final BlobIndexDAO dao;

    /**
     * Connects to the database using the supplied DBI instance. This
     * constructor results in all method calls being committed as soon as
     * possible by jDBI.
     * 
     * The jDBI is a bit vague on making a guarantee that the transaction will
     * be committed. This SqlBlobIndex does not make use of any of the access
     * patterns that would cause the transaction to be held open.
     * 
     * @param dbi
     *            The jDBI instance to access the database through
     */
    public SqlBlobIndex(final IDBI dbi) {
        dbi.onDemand(BlobIndexSchemaDAO.class).createSchema();
    	this.dao = dbi.onDemand(BlobIndexDAO.class);
    }

    /**
     * Connects to the database using a supplied jDBI Handle instance. This
     * constructor allows transaction management to be done by the owner of the
     * Handle.
     * 
     * SqlBlobIndex will not attempt to modify the handle's transaction at all,
     * ie it will not call any methods on
     * org.skife.jdbi.v2.tweak.TransactionHandler
     * 
     * @param h
     *            The connection Handle that will be used to connect to the
     *            database.
     */
    public SqlBlobIndex(final Handle h) {
    	h.attach(BlobIndexSchemaDAO.class).createSchema();
        this.dao = h.attach(BlobIndexDAO.class);
    }

    @Override
    public long locate(final long blobId) throws NoSuchBlobException {
        Long offset = dao.locate(blobId);
        if (offset == null) {
            throw new NoSuchBlobException(new Long(blobId).toString());
        }
        return offset;
    }

    @Override
    public void remember(final long blobId, final long offset) {
        if (dao.locate(blobId) != null) {
            dao.update(blobId, offset);
        } else {
            dao.insert(blobId, offset);
        }
    }

    @Override
    public void delete(final long blobId) {
        dao.delete(blobId);
    }

}
