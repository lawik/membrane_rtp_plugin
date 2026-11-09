defmodule Membrane.SRTCP.DecryptorTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, RemoteStream, RTCP, SRTCP, Testing}

  @key "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  @ssrc 1234

  defp policy(ssrc), do: %ExLibSRTP.Policy{ssrc: ssrc, key: @key}

  defp receiver_report() do
    RTCP.Packet.serialize(%RTCP.ReceiverReportPacket{ssrc: @ssrc, reports: []})
  end

  defp valid_packet() do
    srtp = ExLibSRTP.new()
    :ok = ExLibSRTP.add_stream(srtp, policy(:any_outbound))
    {:ok, protected} = ExLibSRTP.protect_rtcp(srtp, receiver_report())
    protected
  end

  # looks like a receiver report, but the authentication tag doesn't verify
  defp forged_packet(), do: receiver_report() <> :crypto.strong_rand_bytes(14)

  test "drops packets that cannot be unprotected and keeps decrypting valid ones" do
    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Testing.Source{
            output: [forged_packet(), <<>>, valid_packet()],
            stream_format: %RemoteStream{type: :packetized, content_format: RTCP}
          })
          |> child(:decryptor, %SRTCP.Decryptor{policies: [policy(:any_inbound)]})
          |> child(:sink, Testing.Sink)
      )

    receiver_report = receiver_report()
    assert_sink_buffer(pipeline, :sink, %Buffer{payload: ^receiver_report})
    assert_end_of_stream(pipeline, :sink)
    refute_sink_buffer(pipeline, :sink, _buffer, 0)

    Testing.Pipeline.terminate(pipeline)
  end
end
