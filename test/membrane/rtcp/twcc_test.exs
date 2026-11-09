defmodule Membrane.RTCP.TWCCTest do
  use ExUnit.Case, async: true

  alias Membrane.RTCP.Fixtures
  alias Membrane.RTCP.TransportFeedbackPacket.TWCC

  describe "TWCC module" do
    test "encodes and decodes valid feedbacks" do
      encoded_feedbacks = Fixtures.twcc_feedbacks()
      expected_feedbacks = Fixtures.twcc_feedbacks_contents()

      encoded_feedbacks
      |> Enum.zip(expected_feedbacks)
      |> Enum.each(fn {encoded, expected} ->
        assert {:ok, expected} == TWCC.decode(encoded)
        assert encoded == TWCC.encode(expected)
      end)
    end

    test "does not decode malformed feedbacks" do
      encoded_feedbacks = Fixtures.twcc_malformed_feedbacks()

      Enum.each(encoded_feedbacks, fn encoded ->
        assert {:error, :malformed_packet} == TWCC.decode(encoded)
      end)
    end

    test "does not decode feedbacks shorter than the feedback header" do
      assert {:error, :malformed_packet} == TWCC.decode(<<>>)
      assert {:error, :malformed_packet} == TWCC.decode(<<0::16, 1::16, 0::24>>)
    end

    test "does not decode feedbacks with fewer receive deltas than received packets" do
      # three packets with small deltas declared in a run length chunk, only two delta bytes present
      header = <<0::16, 3::16, 0::signed-integer-size(24), 0::8>>
      run_length_chunk = <<0::1, 1::2, 3::13>>
      assert {:error, :malformed_packet} == TWCC.decode(header <> run_length_chunk <> <<1, 2>>)

      # one packet with a large delta declared, only one delta byte present
      header = <<0::16, 1::16, 0::signed-integer-size(24), 0::8>>
      run_length_chunk = <<0::1, 2::2, 1::13>>
      assert {:error, :malformed_packet} == TWCC.decode(header <> run_length_chunk <> <<1>>)
    end
  end
end
