package mammoth.jclient;

/**
 * Created by zzq12 on 2016/9/8.
 */
public class FileInfo {

    private long inputTime = 0L;
    private long flength = 0L;
    private String fileName;

    public FileInfo() {
    }
    
    public FileInfo(long inputTime, long flength, String fileName) {
    	this.inputTime = inputTime;
    	this.flength = flength;
    	this.fileName = fileName;
    }
    
    public long getFlength() {
		return flength;
	}

	public void setFlength(long flength) {
		this.flength = flength;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

    public void setInputTime(long inputTime) {
        this.inputTime = inputTime * 1000;
    }

    public void setLength(long flength) {
        this.flength = flength;
    }

    public long getLength() {
        return flength;
    }

    public long getInputTime() {
        return inputTime;
    }

	@Override
	public String toString() {
		return "FileInfo [inputTime=" + inputTime + ", flength=" + flength + ", fileName=" + fileName + "]";
	}        
    
}
