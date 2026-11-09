defmodule Membrane.RTP.TWCCSenderTest do
  use ExUnit.Case, async: true

  require Membrane.Pad
  alias Membrane.{Buffer, Pad, Time}
  alias Membrane.RTCP.TransportFeedbackPacket.TWCC
  alias Membrane.RTP.TWCCSender

  @pad_id 1
  @ctx %{pads: %{Pad.ref(:input, @pad_id) => %{}, Pad.ref(:output, @pad_id) => %{}}}

  defp feedback(base_seq_num, receive_deltas) do
    %TWCC{
      base_seq_num: base_seq_num,
      reference_time: Time.milliseconds(64),
      packet_status_count: length(receive_deltas),
      receive_deltas: receive_deltas,
      feedback_packet_count: 0
    }
  end

  defp send_packets(state, count) do
    buffer = %Buffer{payload: <<0::1000>>, metadata: %{rtp: %{extensions: [], padding_size: 0}}}

    Enum.reduce(1..count, state, fn _i, state ->
      assert {[buffer: _buffer], state} =
               TWCCSender.handle_buffer(Pad.ref(:input, @pad_id), buffer, @ctx, state)

      state
    end)
  end

  setup do
    {[], state} = TWCCSender.handle_init(nil, nil)
    [state: state]
  end

  test "ignores feedback when no packets have been sent", %{state: state} do
    feedback = feedback(0, [Time.milliseconds(1)])

    assert {[], ^state} =
             TWCCSender.handle_parent_notification({:twcc_feedback, feedback}, @ctx, state)
  end

  test "ignores feedback referencing sequence numbers that were never sent", %{state: state} do
    state = send_packets(state, 2)

    # packets 0 and 1 were sent, feedback also covers 2 and 3
    feedback = feedback(0, List.duplicate(Time.milliseconds(1), 4))

    assert {[], ^state} =
             TWCCSender.handle_parent_notification({:twcc_feedback, feedback}, @ctx, state)

    # feedback for packets before the first one
    feedback = feedback(65_534, List.duplicate(Time.milliseconds(1), 3))

    assert {[], ^state} =
             TWCCSender.handle_parent_notification({:twcc_feedback, feedback}, @ctx, state)

    # feedback not describing any packets
    feedback = feedback(0, [])

    assert {[], ^state} =
             TWCCSender.handle_parent_notification({:twcc_feedback, feedback}, @ctx, state)
  end

  test "processes feedback for sent packets", %{state: state} do
    state = send_packets(state, 3)
    feedback = feedback(0, [Time.milliseconds(1), :not_received, Time.milliseconds(2)])

    assert {[], new_state} =
             TWCCSender.handle_parent_notification({:twcc_feedback, feedback}, @ctx, state)

    assert new_state.cc != state.cc
  end
end
