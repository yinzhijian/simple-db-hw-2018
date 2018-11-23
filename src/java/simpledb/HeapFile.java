package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] data = new byte[pageSize];
        try {
            FileInputStream fis = new FileInputStream(f);
            fis.skip(pid.getPageNumber() * pageSize);
            int result = fis.read(data, 0, pageSize);
            if (result != pageSize) {
                throw new DbException("read page error");
            }
            return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()), data);
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = Database.getBufferPool().getPageSize();
        return (int)(f.length() / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pos=0;
            private Iterator<Tuple> iter;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                PageId pageId = new HeapPageId(getId(),pos);
                HeapPage curPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                iter = curPage.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iter == null) {
                    return false;
                }
                if (iter.hasNext()) {
                    return true;
                }
                if (++pos < numPages()) {
                    PageId pageId = new HeapPageId(getId(),pos);
                    HeapPage curPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                    iter = curPage.iterator();
                }
                if (iter.hasNext()) {
                    return true;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iter == null) {
                    throw new NoSuchElementException();
                }
                return iter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pos = 0;
                PageId pageId = new HeapPageId(getId(),pos);
                HeapPage curPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                iter = curPage.iterator();
            }

            @Override
            public void close() {
                pos = 0;
                iter = null;
            }
        };
    }

}

