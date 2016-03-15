defmodule Poolgirl do
  @type pool :: pid
  @type worker :: pid
  @type waiting_queue :: :queue.queue

  defstruct [
    supervisor: nil,
    workers: [],
    waiting: nil,
    monitors: nil,
    size: 5,
    overflow: 0,
    max_overflow: 10,
    strategy: :lifo
  ]

  @type t :: %__MODULE__{
    supervisor: pid,
    workers: [worker],
    waiting: waiting_queue,
    monitors: :ets.tid,
    size: non_neg_integer,
    overflow: non_neg_integer,
    max_overflow: non_neg_integer,
    strategy: :lifo | :fifo
  }

  def start(pool_args, worker_args) do
    start_pool(:start, pool_args, worker_args)
  end

  def start_link(pool_args, worker_args) do
    start_pool(:start_link, pool_args, worker_args)
  end

  def checkout(pool, block, timeout) do
    cref = Kernel.make_ref
    try do
      GenServer.call(pool, {:checkout, cref, block}, timeout)
    rescue
      exception ->
        stacktrace = System.stacktrace
        GenServer.cast(pool, {:cancel_waiting, cref})
        Kernel.reraise(exception, stacktrace)
      # case class do
      #   :error ->
      #     raise reason
      #   :exit ->
      #     exit reason
      #   :throw ->
      #     throw reason
      # end
    end
  end

  def checkin(pool, worker) when is_pid(worker) do
    GenServer.cast(pool, {:checkin, worker})
  end

  @spec transaction(pool, (worker -> ret), timeout) :: ret when ret: any
  def transaction(pool, action, timeout) do
    worker = __MODULE__.checkout(pool, true, timeout)
    try do
      action.(worker)
    after
      :ok = __MODULE__.checkin(pool, worker)
    end

  end

  def child_spec(pool_id, pool_args, worker_args) do
    options = [
      id: pool_id,
      function: :start_link,
      restart: :permanent,
      shutdown: 5000,
      module: [__MODULE__]
    ]
    Supervisor.Spec.worker(__MODULE__, [pool_args, worker_args], options)
  end

  def stop(pool) do
    GenServer.call(pool, :stop)
  end

  def status(pool) do
    GenServer.call(pool, :status)
  end



  defp start_pool(start_fun, pool_args, worker_args) do
    options = case pool_args |> Keyword.fetch(:name) do
                {:ok, name} -> [name: name]
                :error -> []
              end
    case start_fun do
      :start_link ->
        GenServer.start_link(__MODULE__, {pool_args, worker_args}, options)
      :start ->
        GenServer.start(__MODULE__, {pool_args, worker_args}, options)
    end
  end

  ### GenServer server impl ###

  def init({pool_args, worker_args}) do
    Process.flag(:trap_exit, true)
    waiting = :queue.new
    monitors = :ets.new(:monitors, [:private])
    init(pool_args, worker_args, %__MODULE__{waiting: waiting, monitors: monitors})
  end

  def init([{:worker_module, mod} | rest], worker_args, state) when is_atom(mod) do
    {:ok, sup} = Poolgirl.Supervisor.start_link(mod, worker_args)
    init(rest, worker_args, %{state | supervisor: sup})
  end
  def init([{:size, size} | rest], worker_args, state) when is_integer(size) do
    init(rest, worker_args, %{state | size: size})
  end
  def init([{:max_overflow, max_overflow} | rest], worker_args, state) when is_integer(max_overflow) do
    init(rest, worker_args, %{state | max_overflow: max_overflow})
  end
  def init([{:strategy, :lifo} | rest], worker_args, state) do
    init(rest, worker_args, %{state | strategy: :lifo})
  end
  def init([{:strategy, :fifo} | rest], worker_args, state) do
    init(rest, worker_args, %{state | strategy: :fifo})
  end
  def init([_ | rest], worker_args, state), do: init(rest, worker_args, state)
  def init([], _worker_args, %__MODULE__{
             size: size,
             supervisor: sup
             } = state) do
    workers = for _ <- 1..size, do: new_worker(sup)
    {:ok, %{state | workers: workers}}
  end


  def handle_call({:checkout, cref, block}, {from_pid, _tag} = from, %__MODULE__{
        supervisor: sup,
        workers: workers,
        monitors: monitors,
        overflow: overflow,
        max_overflow: max_overflow
      } = state) do
    case workers do
      [pid | rest] ->
        mref = Process.monitor(from_pid)
        true = :ets.insert(monitors, {pid, cref, mref})
        {:reply, pid, %{state | workers: rest}}
      [] when max_overflow > 0 and overflow < max_overflow ->
        mref = Process.monitor(from_pid)
        pid = new_worker(sup)
        true = :ets.insert(monitors, {pid, cref, mref})
        {:reply, pid, %{state | overflow: overflow + 1}}
      [] when block == false ->
        {:reply, :full, state}
      [] ->
        mref = Process.monitor(from_pid)
        waiting = :queue.in({from, cref, mref}, state.waiting)
        {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(:status, _from,
                  %__MODULE__{
                    workers: workers,
                    monitors: monitors,
                    overflow: overflow
                  } = state) do
    state_name = state_name(state)
    reply = {state_name, length(workers), overflow, :ets.info(monitors, :size)}
    {:reply, reply, state}
  end

  def handle_call(_msg, _from, state) do
    reply = {:error, :invalid_message}
    {:reply, reply, state}
  end


  def handle_cast({:cancel_waiting, cref}, state) do
    case :ets.match(state.monitors, {'$1', cref, '$2'}) do
      [[pid, mref]] ->
        Process.demonitor(mref, [:flush])
        true = :ets.delete(state.monitors, pid)
        new_state = handle_checkin(pid, state)
        {:noreply, new_state}
      [] ->
        cancel = fn
          {_pid, ^cref, mref} ->
            Process.demonitor(mref, [:flush])
          false
          _ ->
            true
        end

        waiting = :queue.filter(cancel, state.waiting)
        {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_cast({:checkin, worker},
                  %__MODULE__{
                    monitors: monitors
                  } = state) do
    case :ets.lookup(monitors, worker) do
      [{^worker, _cref, mref}] ->
        true = Process.demonitor(mref)
        true = :ets.delete(monitors, worker)
        new_state = handle_checkin(worker, state)
        {:noreply, new_state}
      [] ->
        {:noreply, state}
    end

  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end


  def handle_info({:DOWN, mref, :process, _pid, _reason},
                  %__MODULE__{
                    monitors: monitors,
                    waiting: waiting
                  } = state) do
    case :ets.match(monitors, {'$1', '_', mref}) do
      [[pid]] ->
        true = :ets.delete(monitors, pid)
        new_state = handle_checkin(pid, state)
        {:noreply, new_state}
      [] ->
        new_waiting = :queue.filter(fn
          {_, _, ^mref} ->
            false
          _ ->
            true
        end, waiting)
        {:noreply, %{state | waiting: new_waiting}}
    end
  end

  def handle_info({:EXIT, pid, _reason},
                  %__MODULE__{
                    supervisor: sup,
                    monitors: monitors,
                    workers: workers
                  } = state) do
    case :ets.lookup(monitors, pid) do
      [{^pid, _cref, mref}] ->
        true = Process.demonitor(mref)
        true = :ets.delete(monitors, pid)
        new_state = handle_worker_exit(pid, state)
        {:noreply, new_state}
      [] ->
        if Enum.member?(workers, pid) do
          rest = Enum.filter(workers, fn
            ^pid -> false
            _ -> true
          end)
          {:noreply, %{state | workers: [new_worker(sup) | rest]}}
        else
          {:noreply, state}
        end
    end
  end

  def terminate(_reason,
                %__MODULE__{
                  workers: workers,
                  supervisor: sup
                } = _state) do
    :ok = workers |> Enum.each(fn worker -> Process.unlink(worker) end)
    true = Process.exit(sup, :shutdown)
    :ok
  end


  @doc """
  return worker `pid` back to pools
  """
  defp handle_checkin(pid, %{
            supervisor: sup,
            waiting: waiting,
            monitors: monitors,
            overflow: overflow,
            strategy: strategy } = state) do
    case :queue.out(waiting) do
      {{:value, {from, cref, mref}}, left} -> # waiting in the queue
        true = :ets.insert(monitors, {pid, cref, mref})
        GenServer.reply(from, pid)
        %{state | waiting: left}
      {:empty, empty} when overflow > 0 -> # overflow
        :ok = dismiss_worker(sup, pid)
        %{state | waiting: empty, overflow: overflow - 1}
      {:empty, empty} -> # enter the worker pool
        workers = case strategy do
                    :lifo -> [pid | state.workers]
                    :fifo -> state.workers ++ [pid]
                  end
        %{state | workers: workers, waiting: empty, overflow: 0}
    end
  end

  def handle_worker_exit(pid,
                         %__MODULE__{
                           supervisor: sup,
                           monitors: monitors,
                           workers: workers,
                           waiting: waiting,
                           overflow: overflow
                         } = state) do
    case :queue.out(waiting) do
      {{:value, {from, cref, mref}}, left} ->
        new_worker = new_worker(sup)
        true = :ets.insert(monitors, {new_worker, cref, mref})
        GenServer.reply(from, new_worker)
        %{state | waiting: left}
      {:empty, empty} when overflow > 0 ->
        %{state | waiting: empty, overflow: overflow - 1}
      {:empty, empty} ->
        workers = [new_worker(sup) | Enum.filter(workers, fn
                      ^pid -> false
                      _ -> true
                    end)]
        %{state | workers: workers, waiting: empty}
    end
  end


  defp dismiss_worker(sup, pid) do
    true = Process.unlink(pid)
    Supervisor.terminate_child(sup, pid)
  end

  defp new_worker(sup) do
    {:ok, pid} = Supervisor.start_child(sup, [])
    true = Process.link(pid)
    pid
  end

  defp state_name(%__MODULE__{
            overflow: overflow,
            max_overflow: max_overflow,
            workers: workers
              } = _state)
  when overflow < 1 do
    case length(workers) do
      0 when max_overflow < 1 ->
        :full
      0 ->
        :overflow
      _ ->
        :ready
    end
  end
  defp state_name(%__MODULE__{
            overflow: max_overflow,
            max_overflow: max_overflow
              }), do: :full
  defp state_name(_state), do: :overflow
end
