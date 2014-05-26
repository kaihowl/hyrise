// Copyright (c) 2014 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_PIPELINEOBSERVER_H_
#define SRC_LIB_ACCESS_PIPELINEOBSERVER_H_

#include "helper/types.h"
#include "helper/pipelining.h"

#include <log4cxx/logger.h>

#include "taskscheduler/SharedScheduler.h"
#include "taskscheduler/Task.h"
#include "access/system/ResponseTask.h"
#include "access/system/PlanOperation.h"

#include <algorithm>

namespace hyrise {
namespace access {

namespace {
log4cxx::LoggerPtr _observerLogger(log4cxx::Logger::getLogger("pipelining.PipelineObserver"));
}

/*
 * We use the Curiously Recurring Template Pattern below.
 * In order to still be able to dynamic_cast to ALL PipelineObservers,
 * regardless of their template parameter, we had to introduce this abstract class.
 */
class AbstractPipelineObserver {
 public:
  virtual ~AbstractPipelineObserver();
  /*
   * source is the original task, i.e. the parent if the emitter is copied,
   * otherwise the emitter iself.
   */
  virtual void notifyNewChunk(storage::c_aresource_ptr_t chunk, taskscheduler::task_ptr_t emitter) = 0;

  taskscheduler::task_ptr_t getParent() {
    return _parent;
  }

 protected:
  taskscheduler::task_ptr_t _parent;
};

/*
 * Derive from this class to be able to receive (hash) chunks.
 * Fully-pipelining ops have to derive from PipelineObserver and PipelineEmitter.
 * We use CRTP since we have to mixin functionality to some PlanOperations and
 * still be able to use the PlanOperation interface. We did not want to introduce
 * diamond-shaped inheritance. Therefore, we went with CRTP.
 * See docs in "Implementation Details" > "Pipelining"
 */
template <class T>
class PipelineObserver : public AbstractPipelineObserver {
 public:
  virtual void notifyNewChunk(storage::c_aresource_ptr_t chunk, taskscheduler::task_ptr_t source_task) override {
    auto opName = static_cast<T*>(this)->planOperationName();
    auto opId = static_cast<T*>(this)->getOperatorId();

    LOG4CXX_DEBUG(_observerLogger, opId << ": notifyNewChunk");

    // TODO maybe remove the copy from the planop. Make it a clone instead
    // TODO can we make use of the copy constructor / assignment here?
    // TODO that would clean up the code
    auto copy = std::static_pointer_cast<T>(static_cast<T*>(this)->copy());
    copy->setPlanOperationName(opName + "_chunk");
    const auto id = getChunkIdentifier(opId);
    copy->setOperatorId(id);
    auto copy_obs = std::static_pointer_cast<PipelineObserver<T>>(copy);
    copy_obs->_parent = static_cast<T*>(this)->shared_from_this();

    taskscheduler::task_ptr_t original_task;
    // Determine source of chunk
    if (auto source_observer = std::dynamic_pointer_cast<AbstractPipelineObserver>(source_task)) {
      // it might have a parent
      if (auto source_parent = source_observer->getParent()) {
        original_task = source_parent;
      } else {
        original_task = std::dynamic_pointer_cast<taskscheduler::Task>(source_observer);
      }
    } else {
      original_task = source_task;
    }

    copy_obs->_source_index = static_cast<T*>(this)->getDependencyIndex(original_task);

    // input for this new instance is chunk
    copy->addInput(chunk);

    // set dependencies
    // a) this new chunk task is dependency to all successors.
    auto successors = static_cast<T*>(this)->template getAllSuccessorsOf<PlanOperation>();
    std::for_each(successors.begin(), successors.end(), [&copy](std::shared_ptr<PlanOperation>& obs) {
      obs->addDependency(copy);
    });

    // b) allow to add additional dependencies, e.g. hashtable for probes
    addCustomDependencies(copy);

    // schedule task
    auto scheduler = taskscheduler::SharedScheduler::getInstance().getScheduler();
    if (const auto& responseTask = static_cast<T*>(this)->getResponseTask()) {
      responseTask->registerPlanOperation(copy);
    }
    scheduler->schedule(copy);
  }

 protected:
  /*
   * The default implementation sets the newly created task
   * as a dependency of all successors.
   * If you need to implement custom, additional dependencies,
   * do it here.
   */
  virtual void addCustomDependencies(taskscheduler::task_ptr_t newChunkTask) {}

  size_t _source_index;
};
}
}

#endif  // SRC_LIB_ACCESS_PIPELINEOBSERVER_H_
