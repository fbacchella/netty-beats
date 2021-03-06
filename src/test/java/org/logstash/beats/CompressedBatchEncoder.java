package org.logstash.beats;

import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;

public class CompressedBatchEncoder extends BatchEncoder {

    @Override
    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = super.getPayload(ctx, batch);
        ByteBuf outBuffer = ctx.alloc().buffer();
        try {
            try (ByteBufOutputStream output = new ByteBufOutputStream(outBuffer);
                 DeflaterOutputStream outputDeflater = new DeflaterOutputStream(output, new Deflater());) {
                byte[] chunk = new byte[payload.readableBytes()];
                payload.readBytes(chunk);
                outputDeflater.write(chunk);
                outputDeflater.close();
                ByteBuf content = ctx.alloc().buffer(output.writtenBytes());
                content.writeByte(batch.getProtocol());
                content.writeByte(Protocol.CODE_COMPRESSED_FRAME);

                content.writeInt(output.writtenBytes());
                content.writeBytes(output.buffer());
                return content;
            }
        } finally {
            payload.release();
            outBuffer.release();
        }
    }

}
