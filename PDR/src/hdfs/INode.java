package hdfs;

import java.io.Serializable;
import java.util.HashMap;

import formats.Format;

public class INode implements Serializable{
    // Attributs
    private String FileName;
    private Long FileSize;
    private Integer ChunksNumber;
    private HashMap<Integer,HDFSServer> ChunksBlocsID;
    private Format.Type Type;
    // Constructor
    public INode(String vName,Long vSize,Integer vChunksNumber, HashMap<Integer,HDFSServer> vChunksBlocsID,Format.Type vType){
        FileName = vName;
        FileSize = vSize;
        ChunksNumber = vChunksNumber;
        ChunksBlocsID = vChunksBlocsID;
        Type = vType;
    }

    // Get Number of chunks.
    public Integer getNumberOfChunks(){
        return ChunksNumber;
    }

    // Get ChunksBlocsID.
    public HashMap<Integer,HDFSServer> getChunksBlocsID(){
        return ChunksBlocsID;
    }

    // Get File Name.
    public String getFileName(){
        return FileName;
    }

    // Get File Size.
    public Long getFileSize(){
        return FileSize;
    }

    // Add a chunk into the ChunksBlocsID.
    public void addChunk(int n, HDFSServer server) {
        ChunksBlocsID.put(n,server);
    }

    // Get Type of file.
    public Format.Type getType(){
        return Type;
    }
}
