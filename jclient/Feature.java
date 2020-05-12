package mammoth.jclient;

import java.io.Serializable;
import java.util.List;

public class Feature implements Serializable {
    /**
     * ID to serialize
     */
    private static final long serialVersionUID = 6515904314644662715L;

    public enum FeatureType {
        IMAGE_PHASH_ES, IMAGE_LIRE, IMAGE_FACES,
    }

    public enum FeatureLIREType {
        CEDD, AUTO_COLOR_CORRELOGRAM, COLOR_HISTOGRAM, COLOR_LAYOUT,
        EDGE_HISTOGRAM, FCTH, GABOR, CEDD_HASHING, JCD, JOINT_HISTOGRAM,
        JPEG_COEFF_HISTOGRAM, LUMINANCE_LAYOUT, OPPONENT_HISTOGRAM,
        PHOG, SCALABLE_COLOR, TAMURA, SURF, NONE,
        ZH,
    }

    public static FeatureLIREType getFeatureLIREType(String typeStr) {
        FeatureLIREType type = FeatureLIREType.CEDD;

        if (typeStr.equalsIgnoreCase("CEDD")) {
            type = FeatureLIREType.CEDD;
        } else if (typeStr.equalsIgnoreCase("AUTO_COLOR_CORRELOGRAM")) {
            type = FeatureLIREType.AUTO_COLOR_CORRELOGRAM;
        } else if (typeStr.equalsIgnoreCase("COLOR_HISTOGRAM")) {
            type = FeatureLIREType.COLOR_HISTOGRAM;
        } else if (typeStr.equalsIgnoreCase("COLOR_LAYOUT")) {
            type = FeatureLIREType.COLOR_LAYOUT;
        } else if (typeStr.equalsIgnoreCase("EDGE_HISTOGRAM")) {
            type = FeatureLIREType.EDGE_HISTOGRAM;
        } else if (typeStr.equalsIgnoreCase("FCTH")) {
            type = FeatureLIREType.FCTH;
        } else if (typeStr.equalsIgnoreCase("GABOR")) {
            type = FeatureLIREType.GABOR;
        } else if (typeStr.equalsIgnoreCase("CEDD_HASHING")) {
            type = FeatureLIREType.CEDD_HASHING;
        } else if (typeStr.equalsIgnoreCase("JCD")) {
            type = FeatureLIREType.JCD;
        } else if (typeStr.equalsIgnoreCase("JOINT_HISTOGRAM")) {
            type = FeatureLIREType.JOINT_HISTOGRAM;
        } else if (typeStr.equalsIgnoreCase("JPEG_COEFF_HISTOGRAM")) {
            type = FeatureLIREType.JPEG_COEFF_HISTOGRAM;
        } else if (typeStr.equalsIgnoreCase("LUMINANCE_LAYOUT")) {
            type = FeatureLIREType.LUMINANCE_LAYOUT;
        } else if (typeStr.equalsIgnoreCase("OPPONENT_HISTOGRAM")) {
            type = FeatureLIREType.OPPONENT_HISTOGRAM;
        } else if (typeStr.equalsIgnoreCase("PHOG")) {
            type = FeatureLIREType.PHOG;
        } else if (typeStr.equalsIgnoreCase("SCALABLE_COLOR")) {
            type = FeatureLIREType.SCALABLE_COLOR;
        } else if (typeStr.equalsIgnoreCase("TAMURA")) {
            type = FeatureLIREType.TAMURA;
        } else if (typeStr.equalsIgnoreCase("SURF")) {
            type = FeatureLIREType.SURF;
        } else if (typeStr.equalsIgnoreCase("ZH")) {
            type = FeatureLIREType.ZH;
        } else if (typeStr.equalsIgnoreCase("NONE")) {
            type = FeatureLIREType.NONE;
        }

        return type;
    }

    public class FeatureTypeString {
        public static final String IMAGE_PHASH_ES = "image_phash_es";
        public static final String IMAGE_LIRE = "image_lire";
        public static final String IMAGE_FACES = "image_faces";
    }

    public FeatureType type;
    public String value;
    public List<String> args;

    public Feature(FeatureType type) {
        this.type = type;
    }

    public Feature(FeatureType type, List<String> args) {
        this.type = type;
        this.args = args;
    }

    public Feature(FeatureType type, String value) {
        this.type = type;
        this.value = value;
    }

    public Feature(FeatureType feature, List<String> args, String value) {
        this.type = feature;
        this.args = args;
        this.value = value;
    }
}



