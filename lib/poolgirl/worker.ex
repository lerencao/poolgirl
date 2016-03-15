defmodule Poolgirl.Worker do
  @callback start_link(worker_args) ::
  {:ok, pid} | {:error, {:already_started, pid}} | {:error, term}
  when worker_args: Keyword.t
end
