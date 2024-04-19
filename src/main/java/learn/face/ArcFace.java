package learn.face;

import com.arcsoft.face.*;
import com.arcsoft.face.enums.DetectMode;
import com.arcsoft.face.enums.DetectModel;
import com.arcsoft.face.enums.DetectOrient;
import com.arcsoft.face.enums.ErrorInfo;
import com.arcsoft.face.toolkit.ImageInfo;
import com.arcsoft.face.toolkit.ImageInfoEx;
import org.apache.log4j.Logger;
import org.bytedeco.javacv.*;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Point;
import org.bytedeco.opencv.opencv_core.Scalar;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.arcsoft.face.toolkit.ImageFactory.getRGBData;
import static org.bytedeco.opencv.global.opencv_imgproc.CV_AA;
import static org.bytedeco.opencv.global.opencv_imgproc.rectangle;

public class ArcFace {
    private static final Logger logger = Logger.getLogger(JavaCV.class);

    public static void main(String[] args) throws FrameGrabber.Exception {
        String appId = "Fisgvi3GnKP1KxL2Xve5xtUZvKBzk3zEG8wmrNrsqd6o";
        String sdkKey = "F1ZDsN5ekwBizQLVJw23DR4yifBoJNDjiEazFGD1pakP";
        //识别库初始化
        FaceEngine faceEngine = new FaceEngine("D:\\code\\java_learn\\src\\main\\resources\\lib");

        //激活引擎
        int errorCode = faceEngine.activeOnline(appId, sdkKey);
        if (errorCode != ErrorInfo.MOK.getValue() && errorCode != ErrorInfo.MERR_ASF_ALREADY_ACTIVATED.getValue()) {
            System.out.println("引擎激活失败");
        }

        //激活信息
        ActiveFileInfo activeFileInfo=new ActiveFileInfo();
        errorCode = faceEngine.getActiveFileInfo(activeFileInfo);
        if (errorCode != ErrorInfo.MOK.getValue() && errorCode != ErrorInfo.MERR_ASF_ALREADY_ACTIVATED.getValue()) {
            System.out.println("获取激活文件信息失败，返回代码：" + errorCode);
        } else {
            System.out.println("获取激活文件信息成功，返回代码：" + errorCode);
        }
        //引擎配置
        EngineConfiguration engineConfiguration = new EngineConfiguration();
        engineConfiguration.setDetectMode(DetectMode.ASF_DETECT_MODE_IMAGE);
        engineConfiguration.setDetectFaceOrientPriority(DetectOrient.ASF_OP_ALL_OUT);
        engineConfiguration.setDetectFaceMaxNum(10);
        engineConfiguration.setDetectFaceScaleVal(16);
        //功能配置
        FunctionConfiguration functionConfiguration = new FunctionConfiguration();
        functionConfiguration.setSupportAge(true);
        functionConfiguration.setSupportFace3dAngle(true);
        functionConfiguration.setSupportFaceDetect(true);
        functionConfiguration.setSupportFaceRecognition(true);
        functionConfiguration.setSupportGender(true);
        functionConfiguration.setSupportLiveness(true);
        functionConfiguration.setSupportIRLiveness(true);
        engineConfiguration.setFunctionConfiguration(functionConfiguration);


        //初始化引擎
        errorCode = faceEngine.init(engineConfiguration);

        if (errorCode != ErrorInfo.MOK.getValue()) {
            System.out.println("初始化引擎失败");
        }

        //basic(faceEngine);
        //writeFeature(faceEngine);
        openCam(faceEngine);
    }

    public static void basic(FaceEngine faceEngine){
        int errorCode;
        //人脸检测
        ImageInfo imageInfo = getRGBData(new File("./record/me.jpg"));
        List<FaceInfo> faceInfoList = new ArrayList<FaceInfo>();
        errorCode = faceEngine.detectFaces(imageInfo.getImageData(), imageInfo.getWidth(), imageInfo.getHeight(), imageInfo.getImageFormat(), faceInfoList);
        System.out.println(faceInfoList);

        //特征提取
        FaceFeature faceFeature = new FaceFeature();
        errorCode = faceEngine.extractFaceFeature(imageInfo.getImageData(), imageInfo.getWidth(), imageInfo.getHeight(), imageInfo.getImageFormat(), faceInfoList.get(0), faceFeature);
        System.out.println("特征值大小：" + faceFeature.getFeatureData().length);

        //人脸检测2
        ImageInfo imageInfo2 = getRGBData(new File("./record/huang.png"));
        List<FaceInfo> faceInfoList2 = new ArrayList<FaceInfo>();
        errorCode = faceEngine.detectFaces(imageInfo2.getImageData(), imageInfo2.getWidth(), imageInfo2.getHeight(),imageInfo2.getImageFormat(), faceInfoList2);
        System.out.println(faceInfoList2);

        //特征提取2
        FaceFeature faceFeature2 = new FaceFeature();
        errorCode = faceEngine.extractFaceFeature(imageInfo2.getImageData(), imageInfo2.getWidth(), imageInfo2.getHeight(), imageInfo2.getImageFormat(), faceInfoList2.get(0), faceFeature2);
        System.out.println("特征值大小：" + faceFeature2.getFeatureData().length);

        //特征比对
        FaceFeature targetFaceFeature = new FaceFeature();
        targetFaceFeature.setFeatureData(faceFeature.getFeatureData());
        FaceFeature sourceFaceFeature = new FaceFeature();
        sourceFaceFeature.setFeatureData(faceFeature2.getFeatureData());
        FaceSimilar faceSimilar = new FaceSimilar();

        errorCode = faceEngine.compareFaceFeature(targetFaceFeature, sourceFaceFeature, faceSimilar);

        System.out.println("相似度：" + faceSimilar.getScore());

        //设置活体测试
        errorCode = faceEngine.setLivenessParam(0.5f, 0.7f);
        //人脸属性检测
        FunctionConfiguration configuration = new FunctionConfiguration();
        configuration.setSupportAge(true);
        configuration.setSupportFace3dAngle(true);
        configuration.setSupportGender(true);
        configuration.setSupportLiveness(true);
        errorCode = faceEngine.process(imageInfo.getImageData(), imageInfo.getWidth(), imageInfo.getHeight(), imageInfo.getImageFormat(), faceInfoList, configuration);


        //性别检测
        List<GenderInfo> genderInfoList = new ArrayList<GenderInfo>();
        errorCode = faceEngine.getGender(genderInfoList);
        System.out.println("性别：" + genderInfoList.get(0).getGender());

        //年龄检测
        List<AgeInfo> ageInfoList = new ArrayList<AgeInfo>();
        errorCode = faceEngine.getAge(ageInfoList);
        System.out.println("年龄：" + ageInfoList.get(0).getAge());

        //3D信息检测
        List<Face3DAngle> face3DAngleList = new ArrayList<Face3DAngle>();
        errorCode = faceEngine.getFace3DAngle(face3DAngleList);
        System.out.println("3D角度：" + face3DAngleList.get(0).getPitch() + "," + face3DAngleList.get(0).getRoll() + "," + face3DAngleList.get(0).getYaw());

        //活体检测
        List<LivenessInfo> livenessInfoList = new ArrayList<LivenessInfo>();
        errorCode = faceEngine.getLiveness(livenessInfoList);
        System.out.println("活体：" + livenessInfoList.get(0).getLiveness());
    }
    public static void writeFeature(FaceEngine faceEngine){
        int errorCode;
        //引擎配置
        EngineConfiguration engineConfiguration = new EngineConfiguration();
        engineConfiguration.setDetectMode(DetectMode.ASF_DETECT_MODE_IMAGE);//图像模式
        engineConfiguration.setDetectFaceOrientPriority(DetectOrient.ASF_OP_0_ONLY); //逆时针0度
        engineConfiguration.setDetectFaceMaxNum(1);
        engineConfiguration.setDetectFaceScaleVal(32);

        //功能配置
        FunctionConfiguration functionConfiguration = new FunctionConfiguration();
        //functionConfiguration.setSupportAge(true); //年龄检测
        //functionConfiguration.setSupportFace3dAngle(true); //3D角度检测
        functionConfiguration.setSupportFaceDetect(true); //人脸检测
        functionConfiguration.setSupportFaceRecognition(true); //人脸识别
        //functionConfiguration.setSupportGender(true); //性别检测
        functionConfiguration.setSupportLiveness(true); //活体检测
        //functionConfiguration.setSupportIRLiveness(true); //红外活体检测
        engineConfiguration.setFunctionConfiguration(functionConfiguration);

        //获取目录的路径
        //使用相对路径
        //换成绝对路径
        //显示出来
        //把record文件里面的图片抽取特征，按身份证号存在feature文件夹里面
        String recordPath = "./record"; // 模板目录路径
        String featurePath = "./feature"; // 特征目录路径
        File recordDocument = new File(recordPath);//获取路径
        //模板路径的目录不存在的话直接跳出
        if (!recordDocument.exists()) {
            logger.debug("模板图像目录" + recordPath + "不存在");//不存在就输出
            return;
        }
        File fa[] = recordDocument.listFiles();//用数组接收
        //没有文件直接跳出
        if(fa.length == 0){
            logger.debug("目录里面没有图片");
        }

        List<FaceInfo> faceInfoList = new ArrayList<>();
        ImageInfo imageInfo = new ImageInfo();
        ImageInfoEx imageInfoEx = new ImageInfoEx();
        FaceFeature feature = new FaceFeature();
        //逐一提取特征并且保存
        for (File record:fa) {
            logger.debug("提取" + record.toString() + "的特征");
            //imageInfoEx需要imageInfo
            imageInfo = getRGBData(record);

            //extractFaceFeature(byte[] data, int width, int height, ImageFormat imageFormat,
            // FaceInfo faceInfo, FaceFeature feature)

            //extractFaceFeature(ImageInfoEx imageInfoEx, FaceInfo faceInfo, FaceFeature feature)

            //extractFaceFeature(byte[][] imageDataPlanes, int[] imageStrides, int width, int height,
            // ImageFormat imageFormat, FaceInfo faceInfo, FaceFeature feature)
            //三种方法可以提取特征
            //我们选择第二种，需要ImageInfoEx和FaceInfo
            //ImageInfoEx

            imageInfoEx.setHeight(imageInfo.getHeight());
            imageInfoEx.setWidth(imageInfo.getWidth());
            imageInfoEx.setImageFormat(imageInfo.getImageFormat());
            imageInfoEx.setImageDataPlanes(new byte[][]{imageInfo.getImageData()});
            imageInfoEx.setImageStrides(new int[]{imageInfo.getWidth() * 3});
            errorCode = faceEngine.detectFaces(imageInfoEx, DetectModel.ASF_DETECT_MODEL_RGB, faceInfoList);



            //提取feature

            errorCode = faceEngine.extractFaceFeature(imageInfoEx, faceInfoList.get(0), feature);

            //保存特征
            String featureFileName =featurePath + "/" +record.getName() + ".dat";
            File featureFile = new File(featureFileName);
            try {
                OutputStream outputStream = new FileOutputStream(featureFile);
                outputStream.write(feature.getFeatureData());
                outputStream.flush();
                outputStream.close();
            } catch (FileNotFoundException e) {
                logger.debug("创建输出流失败" + featureFileName);
                e.printStackTrace();
            } catch (IOException e) {
                logger.debug("文件写入失败" + featureFileName);
                e.printStackTrace();
            }

            logger.debug(featureFileName);
        }
        //引擎卸载
        errorCode = faceEngine.unInit();
    }
    /**
     *
     * @throws FrameGrabber.Exception
     */
    public static void openCam(FaceEngine faceEngine) throws FrameGrabber.Exception {

        ActiveFileInfo activeFileInfo = new ActiveFileInfo();
        AtomicInteger errorCode = new AtomicInteger(faceEngine.getActiveFileInfo(activeFileInfo));
        if (errorCode.get() != ErrorInfo.MOK.getValue() && errorCode.get() != ErrorInfo.MERR_ASF_ALREADY_ACTIVATED.getValue()) {
            System.out.println("获取激活文件信息失败，返回代码：" + errorCode);
        } else {
            System.out.println("获取激活文件信息成功，返回代码：" + errorCode);
        }

        //引擎配置
        EngineConfiguration engineConfiguration = new EngineConfiguration();
        engineConfiguration.setDetectMode(DetectMode.ASF_DETECT_MODE_IMAGE);//图像模式
        engineConfiguration.setDetectFaceOrientPriority(DetectOrient.ASF_OP_0_ONLY); //逆时针0度
        engineConfiguration.setDetectFaceMaxNum(1);
        engineConfiguration.setDetectFaceScaleVal(32);
        //功能配置
        FunctionConfiguration functionConfiguration = new FunctionConfiguration();
        //functionConfiguration.setSupportAge(true); //年龄检测
        //functionConfiguration.setSupportFace3dAngle(true); //3D角度检测
        functionConfiguration.setSupportFaceDetect(true); //人脸检测
        functionConfiguration.setSupportFaceRecognition(true); //人脸识别
        //functionConfiguration.setSupportGender(true); //性别检测
        functionConfiguration.setSupportLiveness(true); //活体检测
        //functionConfiguration.setSupportIRLiveness(true); //红外活体检测
        engineConfiguration.setFunctionConfiguration(functionConfiguration);


        //初始化引擎
        errorCode.set(faceEngine.init(engineConfiguration));

        if (errorCode.get() != ErrorInfo.MOK.getValue()) {
            System.out.println("初始化引擎失败" + errorCode);
        }

        OpenCVFrameGrabber grabber = new OpenCVFrameGrabber(0);//新建opencv抓取器，一般的电脑和移动端设备中摄像头默认序号是0，不排除其他情况
        grabber.start();//开始获取摄像头数据

        CanvasFrame canvas = new CanvasFrame("摄像头预览");//新建一个预览窗口
        canvas.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);

        //grabber抓的是fram
        //faceEngine.detectFaces 要用到 imageInfo
        //imageInfo要通过getRGBData获得
        //getRGBData(byte[] bytes) getRGBData(InputStream input)
        //byte[] 或者 InputStream 格式的数据都可以用

        //grabber抓的是frame
        //如何将frame转化为 byte[]或者 InputStream

        //目前只能frame转BufferedImage转 byte[]


        Frame frame = new Frame();
        List<FaceInfo> faceInfoList = new ArrayList<FaceInfo>();
        ImageInfo imageInfo = new ImageInfo();
        ByteArrayOutputStream bStream = new ByteArrayOutputStream();
        int countTimes = 0;
        OpenCVFrameConverter converter = new OpenCVFrameConverter.ToMat();
        boolean faceDetected = false;
        Point pointA = new Point();
        Point pointB = new Point();

        //创建feature的list
        List<RegistedFeatureInfo> rfiList = new ArrayList<>();
        //读取feature文件夹里面的数据，写入list里面
        //文件夹位置
        String featurePath = "./feature"; // 特征目录路径
        File featureDocument = new File(featurePath);//获取路径
        //模板路径的目录不存在的话直接跳出
        if (!featureDocument.exists()) {
            System.out.println("人员特征目录" + featurePath + "不存在");//不存在就输出
            return;
        }
        File fa[] = featureDocument.listFiles();//用数组接收
        //没有文件直接跳出
        if (fa.length == 0) {
            System.out.println("没有特征数据");
        }
        //读取
        //is
        for (File feature : fa) {
            try {
                //创建输入流
                InputStream inputStream = new FileInputStream(feature.getAbsoluteFile());
                //创建特征
                RegistedFeatureInfo registedFeatureInfo = new RegistedFeatureInfo();
                FaceFeature faceFeature = new FaceFeature();

                //创建缓存
                byte[] bytes = new byte[faceFeature.FEATURE_SIZE];
                //读数据进缓存
                inputStream.read(bytes);
                //写数据进特征
                faceFeature.setFeatureData(bytes);
                registedFeatureInfo.setFaceFeature(faceFeature);
                registedFeatureInfo.setIdNum(feature.getName());
                //添加到list里面
                rfiList.add(registedFeatureInfo);
                //关闭is
                inputStream.close();
                System.out.println("现有" + rfiList.size() + "人在库");


            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        //窗口是否关闭
        while (canvas.isDisplayable()) {
            /*获取摄像头图像并在窗口中显示,这里Frame frame=grabber.grab()得到是解码后的视频图像*/

            frame = grabber.grab();

            countTimes++;
            if (countTimes % 10 == 0) {
                BufferedImage bufferedImage = Java2DFrameUtils.toBufferedImage(frame);

                try {
                    ImageIO.write(bufferedImage, "jpg", bStream);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                imageInfo = getRGBData(bStream.toByteArray());
                bStream.reset();
            /*
            System.out.println("图片格式:\n" + "高" + imageInfo.getHeight()
                    + "   宽" + imageInfo.getWidth()
                    + "   图片格式" + imageInfo.getImageFormat().toString());
*/

                errorCode.set(faceEngine.detectFaces(imageInfo.getImageData(),
                        imageInfo.getWidth(),
                        imageInfo.getHeight(),
                        imageInfo.getImageFormat(),
                        faceInfoList));
                if (faceInfoList.size() > 0) {
                    //System.out.println(faceInfoList.get(0).getRect());
                    //更新矩形的信息
                    pointA.x(faceInfoList.get(0).getRect().getLeft());
                    pointA.y(faceInfoList.get(0).getRect().getTop());
                    pointB.x(faceInfoList.get(0).getRect().getRight());
                    pointB.y(faceInfoList.get(0).getRect().getBottom());
                    if (pointB.x() - pointA.x() > 150) {
                        faceDetected = true;

                        //提取这个人的特征
                        //特征提取2
                        FaceFeature faceFeature2 = new FaceFeature();
                        faceEngine.extractFaceFeature(imageInfo.getImageData(), imageInfo.getWidth(),
                                imageInfo.getHeight(), imageInfo.getImageFormat(), faceInfoList.get(0), faceFeature2);

                        //看看这个人是谁
                        FaceSimilar faceSimilar = new FaceSimilar();

                        Float fSimilar = 0.0F;
                        String idNumR = "";

                        for (RegistedFeatureInfo person : rfiList) {

                            //对比
                            faceEngine.compareFaceFeature(faceFeature2, person.getFaceFeature(), faceSimilar);

                            //如果faceSimilar大于存储值，更新
                            if (faceSimilar.getScore() > fSimilar) {
                                fSimilar = faceSimilar.getScore();
                                idNumR = person.getIdNum();
                            }

                        }
                        if (fSimilar > 0.8F) {
                            System.out.println("人员对比最高值为" + fSimilar.toString() + "；id为" + idNumR);
                        }

                    } else {
                        faceDetected = false;
                    }


                } else {
                    faceDetected = false;
                }


            }

            //如果有头像
            if (faceDetected) {
                //将frame转换成mat
                Mat mat = Java2DFrameUtils.toMat(frame);
                //加上方框
                //rectangle(grabbedImage, new Point(x, y), new Point(x + w, y + h), Scalar.RED, 1, CV_AA, 0);
                rectangle(mat, pointA, pointB, Scalar.GREEN, 5, CV_AA, 0);
                //mat转换成frame
                frame = converter.convert(mat);
            }


            canvas.showImage(frame);

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        grabber.close();//停止抓取
    }
}
