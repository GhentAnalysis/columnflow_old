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

# Resources

- [columnflow](https://github.com/uhh-cms/columnflow)
- [law](https://github.com/riga/law)
- [order](https://github.com/riga/order)
- [luigi](https://github.com/spotify/luigi)

