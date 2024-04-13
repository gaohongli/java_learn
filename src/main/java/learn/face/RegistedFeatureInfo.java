package learn.face;

import com.arcsoft.face.FaceFeature;

public class RegistedFeatureInfo {
    private String idNum;
    private FaceFeature faceFeature;

    public String getIdNum() {
        return idNum;
    }

    public void setIdNum(String idNum) {
        this.idNum = idNum;
    }

    public FaceFeature getFaceFeature() {
        return faceFeature;
    }

    public void setFaceFeature(FaceFeature faceFeature) {
        this.faceFeature = faceFeature;
    }

    @Override
    public String toString() {
        return "RegistedFeatureInfo{" +
                "idNum='" + idNum + '\'' +
                ", faceFeature=" + faceFeature +
                '}';
    }
}

