# __cf_analysis_name__ Analysis

# Object Definition

All objects collected in [selection/objects.py:object_selection](__cf_module_name__/selection/objects.py#L177).

## Muons

Defined in [selection/objects.py:muon_object](__cf_module_name__/selection/objects.py#L36).

- $|\eta| < 2.4$ 
- $p_T > 10$
- $\texttt{miniPFRelIso all} < 0.4$
- $\texttt{sip3d} < 8$
- $d_{xy} < 0.05$ 
- $d_z < 0.1$

Defined additionally Tight Muons::
- $\texttt{tightId}$

## Electrons

Defined in [selection/objects.py:electron_object](__cf_module_name__/selection/objects.py#L83).

- $|\eta| < 2.5$ 
- $p_T > 15$
- $\texttt{miniPFRelIso all} < 0.4$
- $\texttt{sip3d} < 8$
- $d_{xy} < 0.05$ 
- $d_z < 0.1$
- at most one lost hit 
- is a PF candidate
- with conversion veto applied 
- $\texttt{tightCharge} > 1$
- without a muon closeby ($\\Delta R < 0.05$)

## Jets

Defined in [selection/objects.py:jet_object](__cf_module_name__/selection/objects.py#L132).

- ak4 Jets (standard Jet collection in NanoAOD)
- $|\eta| < 2.5$ 
- $p_T > 30$
- $\texttt{jetId} \\ge 2$
- not containing a muon or lepton ($\\Delta R < 0.4$)


# Calibration

# Event selection

Full default selection flow collected in [selection/default.py:default](__cf_module_name__/selection/default.py#L213).
Aim is to select $t\overline{t}$ events.

- triggers applied in [selection/trigger.py:default](__cf_module_name__/selection/trigger.py#L57)
  - listed in [selection/trigger.py:add_triggers](__cf_module_name__/selection/trigger.py#L11)
- lepton selection applied in [selection/default.py:lepton_selection](__cf_module_name__/selection/default.py#L81).
    - remove Z resonance (same flavour, opposite sign, $|m_{\ell\ell} - 91| < 15$)
    - leading lepton $p_T > 30$
    - subleading lepton $p_T > 20$
    - all leptons in the event should be tight
- jet selection applied in  [selection/default.py:jet_selection](__cf_module_name__/selection/default.py#L136).
  - one b-tagged jet

# Resources

- [columnflow](https://github.com/uhh-cms/columnflow)
- [law](https://github.com/riga/law)
- [order](https://github.com/riga/order)
- [luigi](https://github.com/spotify/luigi)

