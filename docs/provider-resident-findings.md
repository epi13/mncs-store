# Provider/resident substrate audit, 2026-10-02

The legacy Phase 2 boundary test is a slow reference execution path, not a
Store CAS or resident-connection deadlock. With the selected release compiler
and dedicated stdlib, a 180-second bounded run still exceeded its deadline.
A separate 20-second process sample observed four different native experiment
processes making forward progress through chunk operations, each with three
file descriptors. The Python harness remained at thirteen descriptors.
No descriptor growth or stationary subprocess was observed.

The test adapter still defaulted to the removed Language-owned `library`.
It now honors explicit library/stdlib selections and discovers the dedicated
stdlib beside its actual selected compiler. This repairs a separate root
selection defect; it does not claim to solve the reference harness cost.

Normal EmbeddedStore admission/CAS/feed tests remain the acceptance surface
for the resident coordinator. The legacy boundary case needs a separately
owned retained-execution measurement; a timeout is not recorded as PASS.
