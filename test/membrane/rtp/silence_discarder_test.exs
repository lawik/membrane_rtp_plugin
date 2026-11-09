defmodule Membrane.RTP.SilenceDiscarderTest do
  use ExUnit.Case, async: true

  alias Membrane.Buffer
  alias Membrane.RTP.{Header, PacketsDiscardedEvent, SilenceDiscarder}

  @vad_id 1

  defp buffer_with_extension_data(data) do
    extension = %Header.Extension{identifier: @vad_id, data: data}
    %Buffer{payload: <<0, 255>>, metadata: %{rtp: %{extensions: [extension]}}}
  end

  setup do
    {[], state} =
      SilenceDiscarder.handle_init(nil, %SilenceDiscarder{
        max_consecutive_drops: 1000,
        silence_threshold: 127,
        vad_id: @vad_id
      })

    [state: state]
  end

  test "drops silent packets and forwards the others", %{state: state} do
    silent = buffer_with_extension_data(<<1::1, 127::7>>)
    speech = buffer_with_extension_data(<<1::1, 10::7>>)

    assert {[], state} = SilenceDiscarder.handle_buffer(:input, silent, %{}, state)
    assert state.dropped == 1

    assert {[event: {:output, %PacketsDiscardedEvent{discarded: 1}}, buffer: {:output, ^speech}],
            state} = SilenceDiscarder.handle_buffer(:input, speech, %{}, state)

    assert state.dropped == 0
  end

  test "forwards buffers whose VAD extension is not a single byte", %{state: state} do
    for data <- [<<>>, <<1::1, 127::7, 0>>] do
      buffer = buffer_with_extension_data(data)

      assert {[buffer: {:output, ^buffer}], ^state} =
               SilenceDiscarder.handle_buffer(:input, buffer, %{}, state)
    end
  end
end
