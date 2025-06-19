# src/utils/export_results.py

import pandas as pd
from pyomo.environ import value
from pathlib import Path

def export_results(model, path: str = None):
    """
    Export GAMS‐style ResultT, ResultF, ResultA and ResultC tables to Excel,
    plus sum‐pivot summaries for T, F and A.

    Sheets produced:
      - ResultT_all    (hourly: Operation, Volume, Costs_EUR, Startcost_EUR, Variable_OM_cost_EUR)
      - ResultTsum     (sum over hours per (Result,tech), energies as columns)
      - Flows          (hourly: areaFrom, areaTo, energy)
      - ResultFsum     (sum over hours per (areaFrom,areaTo), energies as columns)
      - ResultA_all    (hourly: Buy, Sale, Demand, Import_price_EUR, Export_price_EUR, Buy_EUR, Sale_EUR)
      - ResultAsum     (sum over hours per (Result,area), energies as columns; Electricity‐price rows averaged)
      - ResultC        (hourly CF and summary)
    """
    # 1) compute base path
    if path is None:
        project_root = Path(__file__).parents[2]
        default = project_root / "results" / "Results.xlsx"
    else:
        default = Path(path)
    default.parent.mkdir(parents=True, exist_ok=True)

    base, suffix, folder = default.stem, default.suffix, default.parent

    # 2) build all DataFrames first
    times     = list(model.T)
    time_cols = [str(t) for t in times]
    ntimes    = len(times)

    # --- build hourly ResultT blocks ---
    pairs = set(model.f_in) | set(model.f_out)

    # a) Operation
    op = []
    for g, e in pairs:
        row = {'Result': 'Operation', 'tech': g, 'energy': e}
        for t in times:
            gen = value(model.Generation[g, e, t]) if (g,e) in model.f_out else 0
            use = value(model.Fueluse[g, e, t])      if (g,e) in model.f_in  else 0
            row[str(t)] = gen - use
        op.append(row)
    df_op = pd.DataFrame(op)

    # b) Volume
    vol = []
    for g in model.G_s:
        for e in (f for (gg,f) in model.f_out if gg == g):
            row = {'Result': 'Volume', 'tech': g, 'energy': e}
            for t in times:
                row[str(t)] = value(model.Volume[g, t])
            vol.append(row)
    df_vol = pd.DataFrame(vol)

    # c) Costs_EUR
    cost = []
    for g, e in pairs:
        row = {'Result': 'Costs_EUR', 'tech': g, 'energy': e}
        for t in times:
            imp_qty   = value(model.Fueluse[g, e, t])    if (g,e) in model.f_in  else 0
            sale_qty  = value(model.Generation[g, e, t]) if (g,e) in model.f_out else 0
            imp_price = sum(model.price_buy[a, e, t]  for a in model.A if (a,e) in model.buyE)
            sale_price= sum(model.price_sale[a, e, t] for a in model.A if (a,e) in model.saleE)
            row[str(t)] = imp_qty * imp_price - sale_qty * sale_price
        cost.append(row)
    df_cost = pd.DataFrame(cost)

    # d) Startcost_EUR
    start = []
    for g in model.G:
        row = {'Result': 'Startcost_EUR', 'tech': g, 'energy': 'system_cost'}
        for t in times:
            row[str(t)] = value(model.Startcost[g, t])
        start.append(row)
    df_start = pd.DataFrame(start)

    # e) Variable_OM_cost_EUR
    varom = []
    for g in model.G:
        row = {'Result': 'Variable_OM_cost_EUR', 'tech': g, 'energy': 'system_cost'}
        for t in times:
            row[str(t)] = value(model.Fuelusetotal[g, t]) * model.cvar[g]
        varom.append(row)
    df_varom = pd.DataFrame(varom)

    # sort and concatenate hourly ResultT
    for df in (df_op, df_vol, df_cost, df_start, df_varom):
        df.sort_values(['tech','energy'], inplace=True)
    df_T = pd.concat([df_op, df_vol, df_cost, df_start, df_varom], ignore_index=True)
    df_T = df_T[['Result','tech','energy'] + time_cols]

    # --- build hourly Flows sheet ---
    flows = []
    for ao, ai, f in model.flowset:
        row = {'areaFrom': ao, 'areaTo': ai, 'energy': f}
        for t in times:
            row[str(t)] = value(model.Flow[ao, ai, f, t])
        flows.append(row)
    df_F = pd.DataFrame(flows)
    df_F.sort_values(['areaFrom','areaTo','energy'], inplace=True)
    df_F = df_F[['areaFrom','areaTo','energy'] + time_cols]

    # --- build hourly ResultA sheet ---
    A_rows = []
    # Buy & Sale
    for res, varset in (('Buy', model.buyE), ('Sale', model.saleE)):
        for a, e in varset:
            row = {'Result': res, 'area': a, 'energy': e}
            for t in times:
                row[str(t)] = (value(model.Buy[a,e,t]) if res=='Buy'
                               else value(model.Sale[a,e,t]))
            A_rows.append(row)
    # Demand
    raw_dem   = dict(model.demand.items())
    dem_pairs = sorted({(a,e) for (a,e,t),val in raw_dem.items() if val!=0})
    for a, e in dem_pairs:
        row = {'Result':'Demand','area':a,'energy':e}
        for t in times:
            row[str(t)] = raw_dem.get((a,e,t), 0)
        A_rows.append(row)
    # Import & Export prices
    for res, price_param, sel in (
        ('Import_price_EUR', model.price_buy,  model.buyE),
        ('Export_price_EUR', model.price_sale, model.saleE)
    ):
        for a, e in sel:
            row = {'Result':res,'area':a,'energy':e}
            for t in times:
                row[str(t)] = price_param[a,e,t]
            A_rows.append(row)
    # Buy_EUR & Sale_EUR
    for res, varset, price_param in (
        ('Buy_EUR',  model.buyE,  model.price_buy),
        ('Sale_EUR', model.saleE, model.price_sale),
    ):
        for a, e in varset:
            row = {'Result':res,'area':a,'energy':e}
            for t in times:
                qty   = (value(model.Buy[a,e,t]) if res=='Buy_EUR'
                         else value(model.Sale[a,e,t]))
                price = price_param[a,e,t]
                row[str(t)] = qty * price
            A_rows.append(row)
    df_A = pd.DataFrame(A_rows)
    order = ['Buy','Sale','Demand','Import_price_EUR','Export_price_EUR','Buy_EUR','Sale_EUR']
    df_A['Result'] = pd.Categorical(df_A['Result'], categories=order, ordered=True)
    df_A.sort_values(['Result','area','energy'], inplace=True)
    df_A = df_A[['Result','area','energy'] + time_cols]

    # --- build hourly ResultC (capacity factors) ---
    C_rows = []
    for tech in model.G:
        cap   = value(model.capacity[tech])
        fuels = [e for (g,e) in model.f_out if g==tech]
        row   = {'Result':'CapacityFactor','tech':tech}
        for t in times:
            gen_sum   = sum(value(model.Generation[tech,e,t]) for e in fuels)
            row[str(t)] = gen_sum/cap if cap else 0
        C_rows.append(row)
    df_C_hourly = pd.DataFrame(C_rows, columns=['Result','tech'] + time_cols)

    summary_cf  = []
    summary_flh = []
    for tech in model.G:
        cap    = value(model.capacity[tech])
        fuels  = [e for (g,e) in model.f_out if g==tech]
        total  = sum(value(model.Generation[tech,e,t]) for e in fuels for t in times)
        avg_cf = total/(cap*ntimes) if cap else 0
        summary_cf.append({'Result':'CapacityFactor_Summary','tech':tech,'Average_CF':avg_cf})
        summary_flh.append({'Result':'FullLoadHours','tech':tech,'FLH':avg_cf*ntimes})
    df_C_summary = pd.DataFrame(summary_cf + summary_flh, columns=['Result','tech','Average_CF','FLH'])

    # --- build sum‐pivot summaries ---

    # ResultTsum: sum over hours per (Result,tech), energies as columns
    df_Tsum = (
        df_T
          .set_index(['Result','tech','energy'])[time_cols]
          .sum(axis=1)
          .unstack(fill_value=0)
          .reset_index()
    )
    block_order_T = ['Operation','Volume','Costs_EUR','Startcost_EUR','Variable_OM_cost_EUR']
    df_Tsum['Result'] = pd.Categorical(df_Tsum['Result'], categories=block_order_T, ordered=True)
    df_Tsum.sort_values(['Result','tech'], inplace=True)

    # ResultFsum: sum over hours per (areaFrom,areaTo), energies as columns
    df_Fsum = (
        df_F
          .set_index(['areaFrom','areaTo','energy'])[time_cols]
          .sum(axis=1)
          .unstack(fill_value=0)
          .reset_index()
    )

    # ResultAsum: sum over hours per (Result,area), energies as columns,
    # with Electricity price rows averaged instead of summed
    price_mask = df_A['Result'].isin(['Import_price_EUR','Export_price_EUR'])
    price_rows = df_A[price_mask].copy()
    price_rows['PriceValue'] = price_rows.apply(
        lambda r: r[time_cols].mean() if r['energy']=='Electricity' else r[time_cols[0]],
        axis=1
    )
    df_price = (
        price_rows
          .set_index(['Result','area','energy'])['PriceValue']
          .unstack(level='energy', fill_value=0)
          .reset_index()
    )
    df_nonprice = (
        df_A[~price_mask]
          .set_index(['Result','area','energy'])[time_cols]
          .sum(axis=1)
          .unstack(fill_value=0)
          .reset_index()
    )
    df_Asum = pd.concat([df_nonprice, df_price], ignore_index=True)
    df_Asum['Result'] = pd.Categorical(df_Asum['Result'], categories=order, ordered=True)
    df_Asum.sort_values(['Result','area'], inplace=True)

    # 3) write all sheets in the exact order and with the exact sheet names you asked
    i = 0
    while True:
        filename = f"{base}{'' if i == 0 else f'({i})'}{suffix}"
        output = folder / filename
        try:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # 1) hourly ResultT
                df_T.to_excel(writer, sheet_name='ResultT', index=False)
                # 2) summed ResultT
                df_Tsum.to_excel(writer, sheet_name='ResultTsum', index=False)
                # 3) hourly Flows
                df_F.to_excel(writer, sheet_name='ResultF', index=False)
                # 4) summed Flows
                df_Fsum.to_excel(writer, sheet_name='ResultFsum', index=False)
                # 5) hourly ResultA
                df_A.to_excel(writer, sheet_name='ResultA', index=False)
                # 6) summed ResultA
                df_Asum.to_excel(writer, sheet_name='ResultAsum', index=False)
                # 7) hourly capacity factors
                df_C_hourly.to_excel(writer, sheet_name='ResultC', index=False)
                # 8) summary capacity factors
                df_C_summary.to_excel(writer, sheet_name='ResultCsum', index=False)

            print(f"✅ Wrote all sheets to {output.resolve()}")
            break

        except PermissionError:
            i += 1
            if i > 100:
                raise RuntimeError("Could not write after 100 attempts")
            print(f"⚠️  {output.name} is in use—trying {base}({i}){suffix}…")