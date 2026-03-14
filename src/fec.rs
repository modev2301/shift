//! Forward Error Correction (FEC) using RaptorQ (RFC6330).
//! When the `fec` feature is enabled, chunks can be encoded into source + repair symbols
//! so the receiver can recover from packet loss without retransmit.
//!
//! Wire format for one FEC block: [0x08][OTI 12 bytes][num_packets u32 LE][for each packet: len u32 LE, serialized EncodingPacket].

use crate::error::TransferError;

/// Packet type byte for FEC-encoded block on the wire.
pub const FEC_PACKET_TYPE: u8 = 0x08;

/// OTI (Object Transmission Information) serialized size per RFC6330.
#[allow(dead_code)]
const OTI_LEN: usize = 12;

#[cfg(feature = "fec")]
mod imp {
    use super::*;
    use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};

    /// Default FEC symbol size (MTU-like). RaptorQ uses this as symbol size.
    pub const DEFAULT_FEC_SYMBOL_SIZE: u16 = 1280;

    /// Encode one block of data into source + repair packets.
    /// Returns wire format: [0x08][OTI 12][num_packets u32][len, serialized packet]...
    pub fn encode_block(
        data: &[u8],
        symbol_size: u16,
        repair_packets_per_block: u32,
    ) -> Result<Vec<u8>, TransferError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let config =
            ObjectTransmissionInformation::with_defaults(data.len() as u64, symbol_size);
        let encoder = Encoder::new(data, config);
        let packets = encoder.get_encoded_packets(repair_packets_per_block);
        let mut out = Vec::with_capacity(1 + OTI_LEN + 4 + packets.iter().map(|p| 4 + p.serialize().len()).sum::<usize>());
        out.push(FEC_PACKET_TYPE);
        out.extend_from_slice(&config.serialize());
        out.extend_from_slice(&(packets.len() as u32).to_le_bytes());
        for p in &packets {
            let ser = p.serialize();
            out.extend_from_slice(&(ser.len() as u32).to_le_bytes());
            out.extend_from_slice(&ser);
        }
        Ok(out)
    }

    /// Decode one FEC block from wire format. Call with the full FEC message (including 0x08 and OTI).
    /// Returns decoded data when enough packets received.
    pub fn decode_fec_block(buf: &[u8]) -> Result<Option<Vec<u8>>, TransferError> {
        if buf.len() < 1 + OTI_LEN + 4 {
            return Ok(None);
        }
        if buf[0] != FEC_PACKET_TYPE {
            return Err(TransferError::ProtocolError("Invalid FEC packet type".to_string()));
        }
        let oti_buf: [u8; 12] = buf[1..1 + OTI_LEN]
            .try_into()
            .map_err(|_| TransferError::ProtocolError("FEC OTI length".to_string()))?;
        let config = ObjectTransmissionInformation::deserialize(&oti_buf);
        let num_packets = u32::from_le_bytes(buf[1 + OTI_LEN..1 + OTI_LEN + 4].try_into().unwrap()) as usize;
        let mut decoder = Decoder::new(config);
        let mut offset = 1 + OTI_LEN + 4;
        for _ in 0..num_packets {
            if offset + 4 > buf.len() {
                break;
            }
            let pkt_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + pkt_len > buf.len() {
                break;
            }
            let packet = EncodingPacket::deserialize(&buf[offset..offset + pkt_len]);
            offset += pkt_len;
            decoder.add_new_packet(packet);
        }
        Ok(decoder.get_result())
    }

}

#[cfg(feature = "fec")]
pub use imp::{decode_fec_block, encode_block, DEFAULT_FEC_SYMBOL_SIZE};

#[cfg(not(feature = "fec"))]
pub fn encode_block(
    _data: &[u8],
    _symbol_size: u16,
    _repair_packets_per_block: u32,
) -> Result<Vec<u8>, TransferError> {
    Err(TransferError::ProtocolError(
        "FEC support not compiled in; build with --features fec".to_string(),
    ))
}

#[cfg(not(feature = "fec"))]
pub fn decode_fec_block(_buf: &[u8]) -> Result<Option<Vec<u8>>, TransferError> {
    Ok(None)
}

#[cfg(test)]
#[cfg(feature = "fec")]
mod tests {
    use super::*;

    #[test]
    fn test_fec_encode_decode_roundtrip() {
        let data = b"hello world block for FEC";
        let encoded = encode_block(data, 1280, 2).unwrap();
        assert!(encoded.starts_with(&[FEC_PACKET_TYPE]));
        let decoded = decode_fec_block(&encoded).unwrap();
        assert_eq!(decoded.as_ref().map(|v| v.as_slice()), Some(&data[..]));
    }
}
