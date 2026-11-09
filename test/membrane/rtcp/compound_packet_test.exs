defmodule Membrane.RTCP.PacketTest do
  use ExUnit.Case, async: true

  alias Membrane.RTCP
  alias Membrane.RTCP.Fixtures

  @sample_ssrc 1_374_823_241

  # RTCP header: V=2, P, count/subtype (5 bits), PT, length in 32-bit words
  defp rtcp_header(count, pt, length, padding? \\ false) do
    padding = if padding?, do: 1, else: 0
    <<2::2, padding::1, count::5, pt, length::16>>
  end

  test "compound packets parsing" do
    packet = Fixtures.sample_packet_binary()
    assert {:ok, packets} = RTCP.Packet.parse(packet)

    assert %RTCP.SenderReportPacket{reports: [], sender_info: %{}, ssrc: @sample_ssrc} =
             hd(packets)

    assert %RTCP.ByePacket{ssrcs: [@sample_ssrc]} = Enum.at(packets, 1)
  end

  test "reconstructed packets are (almost) equal to original packets" do
    packet = Fixtures.sample_packet_binary()
    assert {:ok, packets} = RTCP.Packet.parse(packet)
    regenerated_packet = RTCP.Packet.serialize(packets)

    <<head::binary-size(12), ref_ntp_lsw::32, tail::binary>> = packet

    assert <<^head::binary-size(12), ntp_lsw::32, ^tail::binary>> = regenerated_packet

    # The least significant word of NTP timestamp (fractional part) might differ due to rounding errors
    assert_in_delta ntp_lsw, ref_ntp_lsw, 10
  end

  describe "when a malformed packet is contained" do
    test "returns an error" do
      packet = Fixtures.malformed_packet_binary()
      assert {:error, :malformed_packet} = RTCP.Packet.parse(packet)
    end
  end

  describe "when a packet body is shorter than its decoder expects" do
    test "returns an error for a BYE whose count exceeds the body" do
      packet = rtcp_header(5, 203, 1) <> <<@sample_ssrc::32>>
      assert {:error, :malformed_packet} = RTCP.Packet.parse(packet)
    end

    test "returns an error for an APP packet without a name" do
      assert {:error, :malformed_packet} = RTCP.Packet.parse(rtcp_header(0, 204, 1) <> <<0::32>>)
      assert {:error, :malformed_packet} = RTCP.Packet.parse(rtcp_header(0, 204, 0))
    end

    test "returns an error for a Receiver Report without an SSRC" do
      assert {:error, :malformed_packet} = RTCP.Packet.parse(rtcp_header(0, 201, 0))
    end

    test "returns an error for an SDES packet with trailing bytes" do
      # padding leaves a single byte that is too short to be an SSRC
      packet = rtcp_header(0, 202, 1, true) <> <<0xAA, 0, 0, 3>>
      assert {:error, :malformed_sdes} = RTCP.Packet.parse(packet)
    end

    test "returns an error for an SDES chunk without a terminating null item" do
      packet = rtcp_header(1, 202, 1) <> <<@sample_ssrc::32>>
      assert {:error, :malformed_sdes} = RTCP.Packet.parse(packet)
    end

    test "returns an error for an SDES PRIV item without a prefix" do
      packet = rtcp_header(1, 202, 2) <> <<@sample_ssrc::32, 8, 0, 0, 0>>
      assert {:error, :malformed_sdes} = RTCP.Packet.parse(packet)
    end

    test "decodes an SDES PRIV item with a multi-byte value" do
      packet = rtcp_header(1, 202, 3) <> <<@sample_ssrc::32, 8, 5, 1, "p", "abc", 0>>

      assert {:ok, [%RTCP.SdesPacket{chunks: %{@sample_ssrc => chunk}}]} =
               RTCP.Packet.parse(packet)

      assert chunk.priv == {"p", "abc"}
    end

    test "returns an error for a TWCC feedback without a feedback header" do
      packet = rtcp_header(15, 205, 2) <> <<@sample_ssrc::32, @sample_ssrc::32>>
      assert {:error, :malformed_packet} = RTCP.Packet.parse(packet)
    end
  end

  describe "with unknown packet types" do
    test "returns only the parsable packet types" do
      packet = Fixtures.with_unknown_packet_type()
      assert {:ok, packets} = RTCP.Packet.parse(packet)

      assert [
               %RTCP.SenderReportPacket{},
               %RTCP.SdesPacket{},
               %RTCP.FeedbackPacket{}
             ] = packets
    end
  end
end
