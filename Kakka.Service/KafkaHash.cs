namespace Kakka.Service.Kafka;

public static class KafkaHash
{
    public static int Murmur2(byte[] data)
    {
        unchecked // Allow arithmetic overflow
        {
            // Kafka uses int seed = 0x9747b28c in Java,
            // which is actually a negative int if interpreted signed.
            // We store it in a uint literal here, same 32-bit bits.
            uint h = 0x9747b28cU ^ (uint)data.Length;

            // Same 'm' constant in Java
            const uint m = 0x5bd1e995;
            // Right shift distance
            const int r = 24;

            // Process 4 bytes at a time
            int length4 = data.Length >> 2;
            for (int i = 0; i < length4; i++)
            {
                int i4 = i << 2; // i * 4
                uint k =
                    (uint)(data[i4 + 0] & 0xFF)
                    | ((uint)(data[i4 + 1] & 0xFF) << 8)
                    | ((uint)(data[i4 + 2] & 0xFF) << 16)
                    | ((uint)(data[i4 + 3] & 0xFF) << 24);

                k *= m;
                k ^= k >> r; // logical right shift automatically in uint
                k *= m;

                h *= m;
                h ^= k;
            }

            // Handle the remaining bytes
            int remaining = data.Length & 3;
            int offset = length4 << 2;
            if (remaining == 3)
            {
                h ^= (uint)(data[offset + 2] & 0xFF) << 16;
                h ^= (uint)(data[offset + 1] & 0xFF) << 8;
                h ^= (uint)(data[offset] & 0xFF);
                h *= m;
            }
            else if (remaining == 2)
            {
                h ^= (uint)(data[offset + 1] & 0xFF) << 8;
                h ^= (uint)(data[offset] & 0xFF);
                h *= m;
            }
            else if (remaining == 1)
            {
                h ^= (uint)(data[offset] & 0xFF);
                h *= m;
            }

            // Final avalanche
            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            // Cast back to int; bits are unchanged
            return (int)h;
        }
    }
}