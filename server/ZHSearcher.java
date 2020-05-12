package mammoth.server;

import net.semanticmetadata.lire.AbstractImageSearcher;
import net.semanticmetadata.lire.ImageDuplicates;
import net.semanticmetadata.lire.ImageSearchHits;
import net.semanticmetadata.lire.imageanalysis.ColorLayout;
import net.semanticmetadata.lire.imageanalysis.EdgeHistogram;
import net.semanticmetadata.lire.imageanalysis.LireFeature;
import net.semanticmetadata.lire.imageanalysis.ScalableColor;
import net.semanticmetadata.lire.impl.GenericDocumentBuilder;
import net.semanticmetadata.lire.impl.SimpleImageDuplicates;
import net.semanticmetadata.lire.impl.SimpleResult;
import net.semanticmetadata.lire.utils.ImageUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.Bits;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.*;

public class ZHSearcher extends AbstractImageSearcher {
    private int maxHits = 10;
    private float colorHistogramWeight = 1.0f;
    private float colorDistributionWeight = 1.0f;
    private float textureWeight = 1.0f;
    private TreeSet<SimpleResult> docs;
    private LireFeature cachedInstance = null;

    public ZHSearcher(int maxHits) {
        this.maxHits = maxHits;
        this.docs = new TreeSet<SimpleResult>();
    }

    public ZHSearcher(int maxHits, float colorHistogramWeight, float colorDistributionWeight, float textureWeight) {
        this.maxHits = maxHits;
        this.docs = new TreeSet<SimpleResult>();
        this.colorHistogramWeight = colorHistogramWeight;
        this.colorDistributionWeight = colorDistributionWeight;
        this.textureWeight = textureWeight;
    }

    @Override
    public ImageSearchHits search(BufferedImage image, IndexReader reader)
            throws IOException {
        LireFeature sc = null, cl = null, eh = null;

        if (Math.max(image.getHeight(), image.getWidth()) > GenericDocumentBuilder.MAX_IMAGE_DIMENSION) {
            image = ImageUtils.scaleImage(image, GenericDocumentBuilder.MAX_IMAGE_DIMENSION);
        }
        try {
            if (this.colorHistogramWeight > 0.0f) {
                sc = ScalableColor.class.newInstance();
                sc.extract(image);
            }
            if (this.colorDistributionWeight > 0.0f) {
                cl = ColorLayout.class.newInstance();
                cl.extract(image);
            }
            if (this.textureWeight > 0.0f) {
                eh = EdgeHistogram.class.newInstance();
                eh.extract(image);
            }
        } catch (Exception e) {
            throw new IOException(e.getCause() + ", " + e.getMessage());
        }

        float maxDistance = findSimilar(reader, cl, sc, eh);

        return new ZHImageSearchHits(docs, maxDistance, maxHits);
    }

    private float findSimilar(IndexReader reader, LireFeature cl, LireFeature sc,
                              LireFeature eh) throws IOException {
        // Needed for check whether the document is deleted.
        Bits liveDocs = MultiFields.getLiveDocs(reader);
        float maxDistance = -1.0F;
        float overallMaxDistance = -1.0F;

        this.docs.clear();
        int docs = reader.numDocs() + reader.numDeletedDocs();
        for (int i = 0; i < docs; i++) {
            if (reader.hasDeletions() && !liveDocs.get(i)) continue; // if it is deleted, just ignore it.

            Document d = reader.document(i);
            float distance = getDistance(d, cl, sc, eh);

            if (overallMaxDistance < distance) {
                overallMaxDistance = distance;
            }

            if (maxDistance < 0.0F) {
                maxDistance = distance;
            }

            if (this.docs.size() < this.maxHits) {
                this.docs.add(new SimpleResult(distance, d, i));
                if (distance > maxDistance) maxDistance = distance;
            } else if (distance < maxDistance) {
                this.docs.remove(this.docs.last());

                this.docs.add(new SimpleResult(distance, d, i));

                maxDistance = ((SimpleResult) this.docs.last()).getDistance();
            }
        }
        return maxDistance;
    }

    private float getDistance(Document document, LireFeature lireFeature, String fieldName) {
        if (document.getField(fieldName).binaryValue() != null && document.getField(fieldName).binaryValue().length > 0) {
            try {
                cachedInstance = lireFeature.getClass().newInstance();
                cachedInstance.setByteArrayRepresentation(document.getField(fieldName).binaryValue().bytes, document.getField(fieldName).binaryValue().offset, document.getField(fieldName).binaryValue().length);
                return lireFeature.getDistance(cachedInstance);
            } catch (Exception e) {
                System.out.println("Feature newInstance failed! (" + lireFeature.getFeatureName() + ")");
            }
        } else {
            System.out.println("No feature stored in this document! (" + lireFeature.getFeatureName() + ")");
        }
        return 0f;
    }

    private float getDistance(Document d, LireFeature cl, LireFeature sc, LireFeature eh) {
        float distance = 1.0F;
        int descriptorCount = 0;

        if (cl != null && d.getField("descriptorColorLayout") != null) {
            float dist = getDistance(d, cl, "descriptorColorLayout");
            if (dist >= 0) {
                if (dist == 0)
                    dist = 0.0000001f;
                distance *= dist * this.colorDistributionWeight;
                descriptorCount++;
            }
            //System.out.println("CL: " + dist);
        }

        // BUG-XXX: scalable color will give low distance on almost similar background, thus
        // leads to lots of ZEROs. So ...?
        if (sc != null && d.getField("descriptorScalableColor") != null) {
            float dist = getDistance(d, sc, "descriptorScalableColor");
            if (dist >= 0) {
                if (dist == 0)
                    dist = 0.0000001f;
                distance *= dist * this.colorHistogramWeight;
                descriptorCount++;
            }
            //System.out.println("SC: " + dist);
        }

        if (eh != null && d.getField("descriptorEdgeHistogram") != null) {
            float dist = getDistance(d, eh, "descriptorEdgeHistogram");
            if (dist >= 0) {
                if (dist == 0)
                    dist = 0.0000001f;
                distance *= dist * this.textureWeight;
                descriptorCount++;
            }
            //System.out.println("EH: " + dist);
        }

        if (descriptorCount > 0) {
            distance /= descriptorCount * 10;
        }
        //System.out.println("Distance " + distance + ", count = " + descriptorCount);

        return distance;
    }

    @Override
    public ImageSearchHits search(Document doc, IndexReader reader)
            throws IOException {
        LireFeature sc = null, cl = null, eh = null;
        String fieldName = "descriptorScalableColor";

        if (doc.getField(fieldName).binaryValue() != null &&
                doc.getField(fieldName).binaryValue().length > 0) {
            try {
                sc = ScalableColor.class.newInstance();
                sc.setByteArrayRepresentation(doc.getField(fieldName).binaryValue().bytes, doc.getField(fieldName).binaryValue().offset, doc.getField(fieldName).binaryValue().length);
            } catch (Exception e) {
                System.out.println("Get ScalableColor value failed.");
            }
        }
        fieldName = "descriptorColorLayout";
        if (doc.getField(fieldName).binaryValue() != null &&
                doc.getField(fieldName).binaryValue().length > 0) {
            try {
                cl = ColorLayout.class.newInstance();
                cl.setByteArrayRepresentation(doc.getField(fieldName).binaryValue().bytes, doc.getField(fieldName).binaryValue().offset, doc.getField(fieldName).binaryValue().length);
            } catch (Exception e) {
                System.out.println("Get ColorLayout value failed.");
            }
        }
        fieldName = "descriptorEdgeHistogram";
        if (doc.getField(fieldName).binaryValue() != null &&
                doc.getField(fieldName).binaryValue().length > 0) {
            try {
                eh = EdgeHistogram.class.newInstance();
                eh.setByteArrayRepresentation(doc.getField(fieldName).binaryValue().bytes, doc.getField(fieldName).binaryValue().offset, doc.getField(fieldName).binaryValue().length);
            } catch (Exception e) {
                System.out.println("Get EdgeHistogram value failed.");
            }
        }

        float maxDistance = findSimilar(reader, cl, sc, eh);

        return new ZHImageSearchHits(this.docs, maxDistance, this.maxHits);
    }

    @Override
    public ImageDuplicates findDuplicates(IndexReader reader)
            throws IOException {
        Document doc = reader.document(0);
        LireFeature sc = null, cl = null, eh = null;
        String fieldName = "descriptorScalableColor";

        if (doc.getField(fieldName).binaryValue() != null &&
                doc.getField(fieldName).binaryValue().length > 0) {
            try {
                sc = ScalableColor.class.newInstance();
                sc.setByteArrayRepresentation(doc.getField(fieldName).binaryValue().bytes, doc.getField(fieldName).binaryValue().offset, doc.getField(fieldName).binaryValue().length);
            } catch (Exception e) {
                System.out.println("Get ScalableColor value failed.");
            }
        }
        fieldName = "descriptorColorLayout";
        if (doc.getField(fieldName).binaryValue() != null &&
                doc.getField(fieldName).binaryValue().length > 0) {
            try {
                cl = ColorLayout.class.newInstance();
                cl.setByteArrayRepresentation(doc.getField(fieldName).binaryValue().bytes, doc.getField(fieldName).binaryValue().offset, doc.getField(fieldName).binaryValue().length);
            } catch (Exception e) {
                System.out.println("Get ColorLayout value failed.");
            }
        }
        fieldName = "descriptorEdgeHistogram";
        if (doc.getField(fieldName).binaryValue() != null &&
                doc.getField(fieldName).binaryValue().length > 0) {
            try {
                eh = EdgeHistogram.class.newInstance();
                eh.setByteArrayRepresentation(doc.getField(fieldName).binaryValue().bytes, doc.getField(fieldName).binaryValue().offset, doc.getField(fieldName).binaryValue().length);
            } catch (Exception e) {
                System.out.println("Get EdgeHistogram value failed.");
            }
        }

        HashMap<Float, LinkedList<String>> duplicates = new LinkedHashMap<Float, LinkedList<String>>();
        // Needed for check whether the document is deleted.
        Bits liveDocs = MultiFields.getLiveDocs(reader);

        int docs = reader.numDocs();
        int numDuplicates = 0;
        for (int i = 0; i < docs; i++) {
            if (reader.hasDeletions() && !liveDocs.get(i)) continue; // if it is deleted, just ignore it.

            Document d = reader.document(i);
            float distance = getDistance(d, cl, sc, eh);

            if (!duplicates.containsKey(Float.valueOf(distance)))
                duplicates.put(Float.valueOf(distance), new LinkedList<String>());
            else {
                numDuplicates++;
            }
            ((LinkedList<String>) duplicates.get(Float.valueOf(distance))).add(d.getField("descriptorImageIdentifier").stringValue());
        }
        if (numDuplicates == 0) return null;

        List<List<String>> results = new LinkedList<List<String>>();
        for (Iterator<Float> distance = duplicates.keySet().iterator(); distance.hasNext(); ) {
            float f = ((Float) distance.next()).floatValue();
            if (((LinkedList<String>) duplicates.get(Float.valueOf(f))).size() > 1) {
                results.add((LinkedList<String>) duplicates.get(Float.valueOf(f)));
            }
        }
        return new SimpleImageDuplicates(results);
    }

}
