defmodule Membrane.SRTP.DecryptorTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, RTP, SRTP, Testing}
  alias Membrane.RTP.Support.TestSource

  @key "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  @ssrc 1234
  @payload <<1, 2, 3, 4>>
  @rtp_header_size 12

  defp policy(ssrc), do: %ExLibSRTP.Policy{ssrc: ssrc, key: @key}

  defp protect(rtp_packet) do
    srtp = ExLibSRTP.new()
    :ok = ExLibSRTP.add_stream(srtp, policy(:any_outbound))
    {:ok, protected} = ExLibSRTP.protect(srtp, rtp_packet)
    protected
  end

  defp rtp_packet(sequence_number, payload \\ @payload, padding? \\ false) do
    padding = if padding?, do: 1, else: 0

    <<2::2, padding::1, 0::1, 0::4, 0::1, 96::7, sequence_number::16, sequence_number * 100::32,
      @ssrc::32>> <> payload
  end

  defp valid_packet(sequence_number), do: sequence_number |> rtp_packet() |> protect()

  # looks like RTP, but the authentication tag doesn't verify
  defp forged_packet(sequence_number),
    do: rtp_packet(sequence_number, :crypto.strong_rand_bytes(24))

  # decrypts fine, but the padding declared in the last byte is longer than the payload
  defp invalid_padding_packet(sequence_number),
    do: sequence_number |> rtp_packet(<<1, 2, 200>>, true) |> protect()

  defp start_pipeline(packets) do
    buffers =
      Enum.map(packets, fn packet ->
        %Buffer{payload: packet, metadata: %{rtp: %{total_header_size: @rtp_header_size}}}
      end)

    Testing.Pipeline.start_link_supervised!(
      spec:
        child(:source, %TestSource{
          output: TestSource.output_from_buffers(buffers),
          stream_format: %RTP{}
        })
        |> child(:decryptor, %SRTP.Decryptor{policies: [policy(:any_inbound)]})
        |> child(:sink, Testing.Sink)
    )
  end

  test "drops packets that cannot be unprotected and keeps decrypting valid ones" do
    pipeline = start_pipeline([forged_packet(1), invalid_padding_packet(2), valid_packet(3)])

    assert_sink_buffer(pipeline, :sink, %Buffer{payload: @payload})
    assert_end_of_stream(pipeline, :sink)
    refute_sink_buffer(pipeline, :sink, _buffer, 0)

    Testing.Pipeline.terminate(pipeline)
  end

  test "Demuxer with SRTP drops packets that cannot be unprotected" do
    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            output: [forged_packet(1), valid_packet(2)],
            stream_format: %Membrane.RemoteStream{type: :packetized, content_format: RTP}
          })
          |> child(:demuxer, %RTP.Demuxer{srtp: [policy(:any_inbound)]})
      )

    assert_pipeline_notified(pipeline, :demuxer, {:new_rtp_stream, %{ssrc: @ssrc}})
    Testing.Pipeline.terminate(pipeline)
  end
end
