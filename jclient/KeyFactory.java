package mammoth.jclient;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Random;

public class KeyFactory {
    static Random random = new Random();

    public enum KeyType{
        ImageZB("中标图片"),ImageYB("样本图片"),ImagePYQ("朋友圈图片"),ImageJMXF("界面下发图片"),ImageJMFW("界面服务图片"),ImageSJCY("随机采样图片"),
        VideoZB("中标视频"),VideoYB("样本视频"),VideoPYQ("朋友圈视频"),VideoJMXF("界面下发视频"),VideoJMFW("界面服务视频"),VideoSJCY("随机采样视频"),
        AudioZB("中标音频"),AudioYB("样本音频"),AudioPYQ("朋友圈音频"),AudioJMXF("界面下发音频"),AudioJMFW("界面服务音频"),AudioSJCY("随机采样音频"),
        OtherZB("中标文件"),OtherYB("样本文件"),OtherPYQ("朋友圈文件"),OtherJMXF("界面下发文件"),OtherJMFW("界面服务文件"),OtherSJCY("随机采样文件"),
        ZG("王志国wx专用"),WL("王志国wb专用"),
        ImageJGH("广恒图片"),VideoJGH("广恒视频"),
        ImageWB("微博图片"),AudioWB("微博语音"),VideoWB("微博视频"),OtherWB("微博文件"),
        ImageTEST("测试图片"),AudioTEST("测试语音"),VideoTEST("测试视频"),OtherTEST("测试文件");
        String dbName;
        KeyType(String dbName) {
        }

        public String getDbName() {
            return dbName;
        }
    }

    private static String getType(KeyType keyType){
        String type;
        switch (keyType){
            case ImageTEST:
                type = "itest";
                break;
            case AudioTEST:
                type = "atest";
                break;
            case VideoTEST:
                type = "vtest";
                break;
            case OtherTEST:
                type = "otest";
                break;
            case ImageZB:
                type = "iz";
                break;
            case ImageWB:
                type = "iw";
                break;
            case ImagePYQ:
                type = "ip";
                break;
            case ImageJMXF:
                type = "ix";
                break;
            case ImageJMFW:
                type = "if";
                break;
            case ImageSJCY:
                type = "ic";
                break;
            case ImageYB:
                type = "iy";
                break;
            case VideoZB:
                type = "vz";
                break;
            case VideoWB:
                type = "vw";
                break;
            case VideoPYQ:
                type = "vp";
                break;
            case VideoJMXF:
                type = "vx";
                break;
            case VideoJMFW:
                type = "vf";
                break;
            case VideoSJCY:
                type = "vc";
                break;
            case VideoYB:
                type = "vy";
                break;
            case AudioZB:
                type = "az";
                break;
            case AudioWB:
                type = "zqw";
                break;
            case AudioPYQ:
                type = "ap";
                break;
            case AudioJMXF:
                type = "ax";
                break;
            case AudioJMFW:
                type = "af";
                break;
            case AudioSJCY:
                type = "ac";
                break;
            case AudioYB:
                type = "ay";
                break;
            case OtherZB:
                type = "oz";
                break;
            case OtherWB:
                type = "ow";
                break;
            case OtherPYQ:
                type = "op";
                break;
            case OtherJMXF:
                type = "ox";
                break;
            case OtherJMFW:
                type = "of";
                break;
            case OtherSJCY:
                type = "oc";
                break;
            case OtherYB:
                type = "oy";
                break;
            case ZG:
                type = "zg";
                break;
            case WL:
                type = "wl";
                break;
            default:
                type = "";
                break;
        }
        return type;
    }

    /**
     *
     * @param type      数据类型
     * @param md5    数据唯一标识
     * @param unitOfSet  set的时间跨度,单位 S
     * @param bucketNumbers   set中桶的个数
     * @param timeStampSecond 时间戳，单位 S
     * @return key
     */
    public static String getInstance(KeyType type, String md5, int unitOfSet, int bucketNumbers, long timeStampSecond){
        StringBuilder key = new StringBuilder();
        long bucket = timeStampSecond % bucketNumbers;
        long set = timeStampSecond - (timeStampSecond % unitOfSet) + bucket;
        String fileType = getType(type);
        key.append(fileType).append(set).append("@").append(md5);
        return key.toString();
    }

    /**
     *
     * @param type
     * @param md5
     * @param timeStampSecond
     * @return
     */
    public static String getInstance(KeyType type, String md5, long timeStampSecond){
        return getInstance(type, md5, 24*60*60, 24, timeStampSecond);
    }
    /**
     * @param type      数据类型
     * @param md5    数据唯一标识
     * @return
     */
    public static String getInstance(KeyType type, String md5){
        return getInstance(type, md5, Instant.now().getEpochSecond());
    }

    /**
     *
     * @param type
     * @param data
     * @return
     */
    public static String getInstance(KeyType type, byte[] data){
        String md5 = getMd5(data);
        return getInstance(type, md5);
    }

    /**
     *
     * @param bytes
     * @return
     */
    private static String getMd5(byte[] bytes) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("md5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md.update(bytes);
        byte[] mdbytes = md.digest();
        StringBuffer sb = new StringBuffer();
        for (int j = 0; j < mdbytes.length; j++) {
            sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
}
