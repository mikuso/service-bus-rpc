
class TimeoutError extends Error {}
class TimeoutOverrunError extends Error {}
class AbortError extends Error {}
class MethodNotFoundError extends Error {}

module.exports = {
    TimeoutError,
    TimeoutOverrunError,
    AbortError,
    MethodNotFoundError,
};
