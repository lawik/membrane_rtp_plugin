defmodule Membrane.RTP.SSRCRouterTest do
  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad, as: Pad

  alias Membrane.Buffer
  alias Membrane.RTP.Header.Extension
  alias Membrane.RTP.SSRCRouter
  alias Membrane.RTP.Support.TestSource
  alias Membrane.Testing.Pipeline

  # Limits hardcoded in `Membrane.RTP.SSRCRouter`
  @max_unlinked_ssrcs 100
  @max_buffered_packets_per_ssrc 500

  @rtp_metadata_template %{
    csrcs: [],
    extensions: [],
    marker: false,
    padding_size: 0,
    payload_type: 96,
    sequence_number: 1,
    ssrc: 1,
    timestamp: 0,
    total_header_size: 20
  }

  test "New RTP stream" do
    metadata = %{
      @rtp_metadata_template
      | extensions: [%Extension{identifier: 4, data: <<0, 2>>}]
    }

    pipeline = init_pipeline()
    src_send_meta_buffer(pipeline, metadata)

    assert_pipeline_notified(pipeline, :ssrc_router, {:new_rtp_stream, 1, pt, extensions})
    assert pt == metadata.payload_type
    assert extensions == metadata.extensions
    Pipeline.terminate(pipeline)
  end

  test "Wait for required extensions" do
    payload_type = 96

    metadata = %{
      @rtp_metadata_template
      | payload_type: payload_type,
        extensions: [
          %Extension{identifier: 4, data: <<0, 2>>},
          %Extension{identifier: 9, data: "0"}
        ]
    }

    pipeline = init_pipeline()

    send_router_message(pipeline, %SSRCRouter.StreamsInfo{
      require_extensions: %{payload_type => [9, 10]}
    })

    src_send_meta_buffer(pipeline, metadata)

    metadata = %{
      metadata
      | ssrc: 2,
        extensions: [
          %Extension{identifier: 4, data: <<0, 3>>},
          %Extension{identifier: 9, data: "0"},
          %Extension{identifier: 10, data: "l"}
        ]
    }

    src_send_meta_buffer(pipeline, metadata)

    assert_pipeline_notified(
      pipeline,
      :ssrc_router,
      {:new_rtp_stream, 2, ^payload_type, extensions}
    )

    assert extensions == metadata.extensions

    # Ensure the first package was dropped
    refute_pipeline_notified(pipeline, :ssrc_router, {:new_rtp_stream, 1, _pt, _extensions}, 200)

    Pipeline.terminate(pipeline)
  end

  test "Waiting for extensions doesn't break non-simulcast" do
    payload_type = 96

    metadata = %{
      @rtp_metadata_template
      | payload_type: payload_type,
        extensions: [
          %Extension{identifier: 4, data: <<0, 2>>},
          %Extension{identifier: 9, data: "0"}
        ]
    }

    pipeline = init_pipeline()

    send_router_message(pipeline, %SSRCRouter.StreamsInfo{
      require_extensions: %{payload_type => [9, 10]},
      accept_ssrcs: [1]
    })

    src_send_meta_buffer(pipeline, metadata)

    assert_pipeline_notified(
      pipeline,
      :ssrc_router,
      {:new_rtp_stream, 1, ^payload_type, extensions}
    )

    assert extensions == metadata.extensions
    Pipeline.terminate(pipeline)
  end

  test "Bounds the number of packets buffered for a stream awaiting an output pad" do
    pipeline = init_pipeline()

    buffers =
      Enum.map(1..(@max_buffered_packets_per_ssrc + 10), fn seq_num ->
        %Buffer{
          payload: <<seq_num::16>>,
          metadata: %{rtp: %{@rtp_metadata_template | sequence_number: seq_num}}
        }
      end)

    Pipeline.execute_actions(pipeline,
      notify_child: {:source, {:execute_actions, [buffer: {:output, buffers}]}}
    )

    assert_pipeline_notified(pipeline, :ssrc_router, {:new_rtp_stream, 1, _pt, _extensions})
    link_output_pad(pipeline, 1)

    for seq_num <- 1..@max_buffered_packets_per_ssrc do
      assert_sink_buffer(pipeline, {:sink, 1}, %Buffer{
        metadata: %{rtp: %{sequence_number: ^seq_num}}
      })
    end

    refute_sink_buffer(pipeline, {:sink, 1}, _buffer, 200)
    Pipeline.terminate(pipeline)
  end

  test "Bounds the number of streams awaiting an output pad" do
    pipeline = init_pipeline()

    for ssrc <- 1..(@max_unlinked_ssrcs + 5) do
      src_send_meta_buffer(pipeline, %{@rtp_metadata_template | ssrc: ssrc})
    end

    for ssrc <- 1..@max_unlinked_ssrcs do
      assert_pipeline_notified(pipeline, :ssrc_router, {:new_rtp_stream, ^ssrc, _pt, _extensions})
    end

    refute_pipeline_notified(pipeline, :ssrc_router, {:new_rtp_stream, _ssrc, _pt, _ext}, 200)

    # Linking a pad makes room for another stream
    link_output_pad(pipeline, 1)
    assert_sink_buffer(pipeline, {:sink, 1}, %Buffer{metadata: %{rtp: %{ssrc: 1}}})

    new_ssrc = @max_unlinked_ssrcs + 10
    src_send_meta_buffer(pipeline, %{@rtp_metadata_template | ssrc: new_ssrc})

    assert_pipeline_notified(
      pipeline,
      :ssrc_router,
      {:new_rtp_stream, ^new_ssrc, _pt, _extensions}
    )

    # Streams explicitly accepted by the parent are not subject to the limit
    accepted_ssrc = @max_unlinked_ssrcs + 20

    send_router_message(pipeline, %SSRCRouter.StreamsInfo{
      require_extensions: %{},
      accept_ssrcs: [accepted_ssrc]
    })

    src_send_meta_buffer(pipeline, %{@rtp_metadata_template | ssrc: accepted_ssrc})

    assert_pipeline_notified(
      pipeline,
      :ssrc_router,
      {:new_rtp_stream, ^accepted_ssrc, _pt, _extensions}
    )

    Pipeline.terminate(pipeline)
  end

  defp init_pipeline() do
    Pipeline.start_link_supervised!(
      spec:
        child(:source, %TestSource{output: [], stream_format: %Membrane.RTP{}})
        |> child(:ssrc_router, SSRCRouter)
    )
  end

  defp src_send_meta_buffer(pipeline, metadata) do
    buffer = %Buffer{payload: "", metadata: %{rtp: metadata}}

    Pipeline.execute_actions(pipeline,
      notify_child: {:source, {:execute_actions, [buffer: {:output, buffer}]}}
    )
  end

  defp link_output_pad(pipeline, ssrc) do
    Pipeline.execute_actions(pipeline,
      spec:
        get_child(:ssrc_router)
        |> via_out(Pad.ref(:output, ssrc))
        |> child({:sink, ssrc}, Membrane.Testing.Sink)
    )
  end

  defp send_router_message(pipeline, msg) do
    Pipeline.execute_actions(pipeline, notify_child: {:ssrc_router, msg})
  end
end
