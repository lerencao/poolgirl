defmodule Poolgirl.Supervisor do
  def start_link(worker_mod, worker_args) do
    Supervisor.start_link(__MODULE__, {worker_mod, worker_args})
  end

  def init({mod, args}) do
    options = [
      restart: :temporary,
      shutdown: 5000
    ]
    worker = Supervisor.Spec.worker(mod, [args], options)
    Supervisor.Spec.supervise([worker], strategy: :simple_one_for_one, max_restarts: 0, max_seconds: 1)
  end

end
