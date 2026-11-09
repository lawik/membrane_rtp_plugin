defmodule Membrane.RTP.DemuxerTest do
  @moduledoc false
  use ExUnit.Case
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.Testing

  @rtp_input %{
    pcap_path: "test/fixtures/rtp/session/demo_rtp.pcap",
    audio: %{ssrc: 439_017_412, packets: 20, payload_type: 127},
    video: %{ssrc: 670_572_639, packets: 842, payload_type: 96}
  }

  defmodule Reorderer do
    use Membrane.Filter

    def_input_pad :input, accepted_format: _any
    def_output_pad :output, accepted_format: _any

    @impl true
    def handle_init(_ctx, _opts) do
      {[], %{buffers: []}}
    end

    @impl true
    def handle_buffer(:input, buffer, _ctx, state) do
      case state.buffers do
        [first_buffer, second_buffer] ->
          {[buffer: {:output, [first_buffer, buffer, second_buffer]}], %{buffers: []}}

        buffer_list ->
          {[], %{state | buffers: buffer_list ++ [buffer]}}
      end
    end

    @impl true
    def handle_end_of_stream(:input, _ctx, state) do
      {[buffer: {:output, state.buffers}, end_of_stream: :output], state}
    end
  end

  defmodule UpfrontPayloadTypePipeline do
    use Membrane.Pipeline

    @impl true
    def handle_init(_ctx, opts) do
      spec = [
        child(:pcap_source, %Membrane.Pcap.Source{path: opts.pcap_path})
        |> then(&if opts.reorder_packets, do: child(&1, Reorderer), else: &1)
        |> child(:demuxer, Membrane.RTP.Demuxer)
        |> via_out(:output,
          options: [
            stream_id: {:payload_type, opts.audio.payload_type},
            jitter_buffer_latency: Membrane.Time.milliseconds(5)
          ]
        )
        |> child({:sink, opts.audio.ssrc}, Testing.Sink),
        get_child(:demuxer)
        |> via_out(:output,
          options: [
            stream_id: {:payload_type, opts.video.payload_type},
            jitter_buffer_latency: Membrane.Time.milliseconds(5)
          ]
        )
        |> child({:sink, opts.video.ssrc}, Testing.Sink)
      ]

      {[spec: spec], %{}}
    end
  end

  defmodule UpfrontEncodingNamePipeline do
    use Membrane.Pipeline

    @payload_type_mapping %{
      96 => %{encoding_name: :Oldman, clock_rate: 90_000},
      127 => %{encoding_name: :Sysy, clock_rate: 48_000}
    }

    @impl true
    def handle_init(_ctx, opts) do
      spec = [
        child(:pcap_source, %Membrane.Pcap.Source{path: opts.pcap_path})
        |> child(:demuxer, %Membrane.RTP.Demuxer{payload_type_mapping: @payload_type_mapping})
        |> via_out(:output,
          options: [
            stream_id: {:encoding_name, :Sysy},
            jitter_buffer_latency: Membrane.Time.milliseconds(5)
          ]
        )
        |> child({:sink, opts.audio.ssrc}, Testing.Sink),
        get_child(:demuxer)
        |> via_out(:output,
          options: [
            stream_id: {:encoding_name, :Oldman},
            jitter_buffer_latency: Membrane.Time.milliseconds(5)
          ]
        )
        |> child({:sink, opts.video.ssrc}, Testing.Sink)
      ]

      {[spec: spec], %{}}
    end
  end

  defmodule DynamicPipeline do
    use Membrane.Pipeline

    @impl true
    def handle_init(_ctx, opts) do
      spec =
        child(:pcap_source, %Membrane.Pcap.Source{path: opts.pcap_path})
        |> then(&if opts.reorder_packets, do: child(&1, Reorderer), else: &1)
        |> child(:demuxer, Membrane.RTP.Demuxer)

      {[spec: spec], %{}}
    end

    @impl true
    def handle_child_notification({:new_rtp_stream, %{ssrc: ssrc}}, :demuxer, _ctx, state) do
      spec =
        get_child(:demuxer)
        |> via_out(:output,
          options: [
            stream_id: {:ssrc, ssrc},
            jitter_buffer_latency: Membrane.Time.milliseconds(5)
          ]
        )
        |> child({:sink, ssrc}, Testing.Sink)

      {[spec: spec], state}
    end
  end

  defp perform_test(pipeline_module, reorder_packets \\ false) do
    pipeline =
      Testing.Pipeline.start_supervised!(
        module: pipeline_module,
        custom_args: Map.put(@rtp_input, :reorder_packets, reorder_packets)
      )

    %{audio: %{ssrc: audio_ssrc}, video: %{ssrc: video_ssrc}} =
      @rtp_input

    assert_start_of_stream(pipeline, :demuxer)
    assert_start_of_stream(pipeline, {:sink, ^video_ssrc})
    assert_start_of_stream(pipeline, {:sink, ^audio_ssrc})

    1..@rtp_input.video.packets
    |> Enum.each(fn _i ->
      assert_sink_buffer(pipeline, {:sink, video_ssrc}, %Membrane.Buffer{})
    end)

    1..@rtp_input.audio.packets
    |> Enum.each(fn _i ->
      assert_sink_buffer(pipeline, {:sink, audio_ssrc}, %Membrane.Buffer{})
    end)

    assert_end_of_stream(pipeline, {:sink, ^video_ssrc})
    assert_end_of_stream(pipeline, {:sink, ^audio_ssrc})
    Testing.Pipeline.terminate(pipeline)
  end

  defmodule NotifyingPipeline do
    # Pipeline that doesn't link any output pad of the demuxer on its own
    use Membrane.Pipeline

    @impl true
    def handle_init(_ctx, opts) do
      spec =
        child(:source, %Membrane.RTP.Support.TestSource{
          output: [],
          stream_format: %Membrane.RemoteStream{type: :packetized, content_format: Membrane.RTP}
        })
        |> child(:demuxer, %Membrane.RTP.Demuxer{
          not_linked_pad_handling: opts.not_linked_pad_handling
        })

      {[spec: spec], %{}}
    end
  end

  describe "Demuxer demuxes correct amount of packets" do
    test "when it's pads were linked upfront based on payload types" do
      perform_test(UpfrontPayloadTypePipeline)
    end

    test "when it's pads were linked upfront based on encoding names" do
      perform_test(UpfrontEncodingNamePipeline)
    end

    test "when it's pads were linked based on notifications received" do
      perform_test(DynamicPipeline)
    end

    test "when packets are reordered in a pipeline linked upfront" do
      perform_test(UpfrontPayloadTypePipeline, true)
    end

    test "when packets are reordered in a dynamic pipeline" do
      perform_test(DynamicPipeline, true)
    end
  end

  describe "Demuxer, when the pad for a stream is not linked" do
    @ssrc 12_345
    @timeout_ms 100

    test "forgets the stream after the timeout and announces it again" do
      pipeline = start_notifying_pipeline(:ignore)

      send_rtp_packet(pipeline, 1)
      assert_pipeline_notified(pipeline, :demuxer, {:new_rtp_stream, %{ssrc: @ssrc}})
      refute_pipeline_notified(pipeline, :demuxer, {:new_rtp_stream, _stream}, @timeout_ms * 3)

      demuxer_pid = Testing.Pipeline.get_child_pid!(pipeline, :demuxer)
      assert %{stream_states: stream_states} = :sys.get_state(demuxer_pid).internal_state
      assert stream_states == %{}

      send_rtp_packet(pipeline, 2)
      assert_pipeline_notified(pipeline, :demuxer, {:new_rtp_stream, %{ssrc: @ssrc}})
      link_output_pad(pipeline)

      assert_sink_buffer(pipeline, :sink, %Membrane.Buffer{
        metadata: %{rtp: %{sequence_number: 2}}
      })

      Testing.Pipeline.terminate(pipeline)
    end

    test "still warns about the stream once" do
      pipeline = start_notifying_pipeline(:warn)
      demuxer_pid = Testing.Pipeline.get_child_pid!(pipeline, :demuxer)

      send_rtp_packet(pipeline, 1)
      assert_pipeline_notified(pipeline, :demuxer, {:new_rtp_stream, %{ssrc: @ssrc}})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          refute_pipeline_notified(
            pipeline,
            :demuxer,
            {:new_rtp_stream, _stream},
            @timeout_ms * 3
          )

          assert %{stream_states: %{}} = :sys.get_state(demuxer_pid).internal_state
        end)

      assert log =~ "Pad corresponding to ssrc #{@ssrc} not connected within specified timeout"
      Testing.Pipeline.terminate(pipeline)
    end

    defp start_notifying_pipeline(action) do
      Testing.Pipeline.start_link_supervised!(
        module: NotifyingPipeline,
        custom_args: %{
          not_linked_pad_handling: %{
            action: action,
            timeout: Membrane.Time.milliseconds(@timeout_ms)
          }
        }
      )
    end

    defp send_rtp_packet(pipeline, sequence_number) do
      payload =
        ExRTP.Packet.new(<<1, 2, 3>>,
          payload_type: 96,
          sequence_number: sequence_number,
          timestamp: sequence_number * 100,
          ssrc: @ssrc
        )
        |> ExRTP.Packet.encode()

      Testing.Pipeline.execute_actions(pipeline,
        notify_child:
          {:source, {:execute_actions, [buffer: {:output, %Membrane.Buffer{payload: payload}}]}}
      )
    end

    defp link_output_pad(pipeline) do
      Testing.Pipeline.execute_actions(pipeline,
        spec:
          get_child(:demuxer)
          |> via_out(:output, options: [stream_id: {:ssrc, @ssrc}, jitter_buffer_latency: 0])
          |> child(:sink, Testing.Sink)
      )
    end
  end
end
