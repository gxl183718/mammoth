package mammoth.server;

public class WriteTask {
    private String set;
    private String md5;
    private byte[] content;
    private int coff;
    private int clen;
    private long flen;
    private String result;
    private String fname;

    public WriteTask(String set, String md5, byte[] content, int coff, int clen) {
        this.set = set;
        this.md5 = md5;
        this.content = content;
        this.coff = coff;
        this.clen = clen;
    }

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public int getCoff() {
        return coff;
    }

    public void setCoff(int coff) {
        this.coff = coff;
    }

    public int getClen() {
        return clen;
    }

    public void setClen(int clen) {
        this.clen = clen;
    }

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }
    public long getFlen() {
        return flen;
    }

    public void setFlen(long flen) {
        this.flen = flen;
    }
}
