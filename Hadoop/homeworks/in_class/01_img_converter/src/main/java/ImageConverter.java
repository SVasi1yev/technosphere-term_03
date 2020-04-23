import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ImageConverter {
    public static byte[] Convert(byte[] data, String format) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedImage image = ImageIO.read(in);

        ImageIO.write(image, format, out);
        return out.toByteArray();
    }
}
