using System;
using System.Collections.Generic;
using System.IO;
using Energistics.DataAccess;
using Energistics.DataAccess.WITSML141;
using Energistics.DataAccess.WITSML141.ComponentSchemas;
using System.Linq;
using System.Data;
using Energistics.DataAccess.WITSML141.ReferenceData;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using System.Data.Entity;

namespace ExportWITSML
{
    class DataExporter
    {
        private Well well { get; set; }
        private MudReport mudreport { get; set; }

        private string ActiveFluidTech { get; set; }

        private List<MudCheck> mudChecks { get; set; }

        private string dbConn { get; set; }

        private enum ReportType
        {
            MudCheckProperty,
            InventoryUsage
        }

        public DataExporter(Well w, MudReport mr)
        {
            dbConn = DBConnection.GetConn();
            well = w;
            SetActiveFluidTech(w);
            mudreport = mr;
            LoadMudChecks();
            
        }

        private void SetActiveFluidTech(Well w)
        {
            try
            {
                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    List<FluidTechWellJCT> fluidTechJCTs = (from ftJCT in db.FluidTechWellJCTs
                                                            where ftJCT.IDWell == w.UIDWell
                                                            select ftJCT
                                                   ).OrderBy(x => x.StartDate).ToList();
                    if (fluidTechJCTs.Count > 0)
                    {
                        FluidTechWellJCT fluidTechJCT = fluidTechJCTs.LastOrDefault();
                        ActiveFluidTech = fluidTechJCT.FluidsTech.FirstName + " " + fluidTechJCT.FluidsTech.LastName;
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private void LoadMudChecks()
        {
            using (var db = new EnerTraxDeployEntities(dbConn))
            {
                mudChecks = (from mc in db.MudChecks
                             where mc.IDMudReport == mudreport.UIDMudReport
                             orderby mc.MudcheckNo
                             select mc).ToList();
            }
        }

        private string GetFileName(string reportName)
        {
            string fileName = well.WellName + " Rpt #" + mudreport.ReportNo + " " + reportName + " " + DateTime.Now.ToShortDateString();
            fileName = RemoveIllegalChar(fileName);
            fileName = fileName + ".xml";
            return fileName;
        }

        private string RemoveIllegalChar(string strFileName)
        {
            string strName = string.Empty;
            try
            {
                strName = Regex.Replace(strFileName, @"\<|\>|\:|\"" |\/|\\|\||\?|\*|\.", "-").Trim();
            }
            catch
            {

            }

            return strName;
        }

        private string GetFileName(string reportName, string mudCheckNo)
        {
            string fileName = well.WellName + " Rpt #" + mudreport.ReportNo + " Mud Check #" + mudCheckNo + " " + reportName + " " + DateTime.Now.ToShortDateString() + ".xml";
            return fileName;
        }

        public void exportFile(string reportName)
        {
            FolderBrowserDialog fbd = new FolderBrowserDialog();
            fbd.SelectedPath = String.IsNullOrEmpty(Properties.Settings.Default.FolderPath) ? Environment.GetFolderPath(Environment.SpecialFolder.Desktop) : Properties.Settings.Default.FolderPath;

            if (fbd.ShowDialog() == DialogResult.OK)
            {
                Properties.Settings.Default.FolderPath = fbd.SelectedPath;
                Properties.Settings.Default.Save();

                //WriteToFile(fbd.SelectedPath, ReportType.InventoryUsage);
                WriteToFile(fbd.SelectedPath, reportName, ReportType.MudCheckProperty);

                MessageBox.Show("Files saved.");        
            }
        }

        private void WriteToFile(string directoryPath, string reportName, ReportType reportType)
        {
            

            if(reportName == "Fluids")
            {
                GenerateXMLFluidsReport(directoryPath, reportName);
            }
            else if(reportName == "Ops")
            {
                StreamWriter writer = new StreamWriter(directoryPath + "\\" + GetFileName(reportName));
                writer.WriteLine(GenerateXML(reportType));
                writer.Dispose();
                writer.Close();
            }

            

        }

        private List<Inventory> MudInventoryUsageFill()
        {
            List<Inventory> mudInv = new List<Inventory>();
            List<ProductTransactionJCT> deliv = null;
            List<ProductTransactionJCT> credit = null;
            using (var db = new EnerTraxDeployEntities(dbConn))
            {
                var query = (from u in db.ProductUsages
                             join uJCT in db.ProductWellUsageJCTs on u.UIDProductUsage equals uJCT.IDProductUsage
                             join se in db.Sections on u.IDSection equals se.UIDSection
                             join wp in db.WellProducts on uJCT.IDWellProducts equals wp.UIDWellProducts
                             join p in db.Products on wp.IDProduct equals p.UIDProduct
                             join pu in db.ProductUnits on p.IDProductUnit equals pu.UIDProductUnit into unitsGrp
                             from itemUnit in unitsGrp.DefaultIfEmpty()
                             join pt in db.ProductTypes on p.IDProductType equals pt.UIDProductType into typeGrp
                             where se.IDWell== well.UIDWell && u.UsageDate <= mudreport.InventoryEndDate && u.UsageDate >= mudreport.InventoryStartDate
                             from itemType in typeGrp.DefaultIfEmpty()
                             group uJCT by new { uJCT.UIDProductWellUsageJCT, wp.UIDWellProducts, wp.Price, p.ProductName, p.Weight, wp.Density} into productsGrp
                             select new {
                                 UID = productsGrp.Key.UIDProductWellUsageJCT,
                                 IDWellProduct = productsGrp.Key.UIDWellProducts,
                                 TotalUsage = productsGrp.Sum(x => x.Amount),
                                 ProductName = productsGrp.Key.ProductName,
                                Price = productsGrp.Key.Price,
                                 Weight = productsGrp.Key.Weight,
                                 Density = productsGrp.Key.Density
                             }
                             );

                deliv = (from t in db.ProductTransactions
                         join tJCT in db.ProductTransactionJCTs on t.UIDProductTransactions equals tJCT.IDProductTransaction
                         join tlTo in db.TransactionLocations on t.IDTransactionLocationTo equals tlTo.UIDTransactionLocation
                         join p in db.Products on tJCT.IDProduct equals p.UIDProduct
                         where tlTo.IDLocation == well.UIDWell
                                && t.TransactionDate >= mudreport.InventoryStartDate
                                && t.TransactionDate <= mudreport.InventoryEndDate
                         select tJCT
                        ).Include("ProductTransaction").ToList();

                credit = (from t in db.ProductTransactions
                          join tJCT in db.ProductTransactionJCTs on t.UIDProductTransactions equals tJCT.IDProductTransaction
                          join tlTo in db.TransactionLocations on t.IDTransactionLocationTo equals tlTo.UIDTransactionLocation
                          join p in db.Products on tJCT.IDProduct equals p.UIDProduct
                          where tlTo.IDLocation == well.UIDWell
                                 && t.TransactionDate >= mudreport.InventoryStartDate
                                 && t.TransactionDate <= mudreport.InventoryEndDate
                          select tJCT
                        ).Include("ProductTransaction").ToList();

                foreach (var row in query)
                {
                    Inventory inv = new Inventory();
                    inv.Uid = row.UID.ToString();
                    inv.Name = row.ProductName;

                    if (row.TotalUsage != null)
                        inv.QtyUsed = (short)row.TotalUsage;

                    if (row.Price != null)
                        inv.PricePerUnit = CreateCost((double)row.Price);

                    if(row.Weight !=null)
                        inv.ItemWeightPerUnit = CreateMassMeasure(MassUom.kg, (double)row.Weight);

                    decimal? pDeliv = deliv.Where(x => x.Active == true && x.IDProduct == row.UID).Sum(x => x.Amount);
                    if (pDeliv != null && pDeliv != 0)
                        inv.QtyReceived = (short)pDeliv;
                    decimal? pCredit = deliv.Where(x => x.Active == true && x.IDProduct == row.UID).Sum(x => x.Amount);
                    if (pCredit != null && pCredit != 0)
                        inv.QtyReturned = (short)pCredit;

                    mudInv.Add(inv);
                }
            }

            return mudInv;
                         
        }

        private Cost CreateCost(double val)
        {
            Cost c = new Cost();
            try
            {
                c.Currency = "cdn";
                c.Value = val;
            }
            catch
            {

            }
            return c;
        }

        private VolumeMeasure CreateVolMeasure(string val, VolumeUom uom)
        {
            VolumeMeasure vm = new VolumeMeasure();
            try
            {
                vm.Uom = uom;
                vm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
           
            return vm;
        }

        private RelativePowerMeasure CreateRelativePowerMeasure(double val, RelativePowerUom rpu)
        {
            RelativePowerMeasure rpm = new RelativePowerMeasure();
            try
            {
                rpm.Uom = rpu;
                rpm.Value = val;
            }
            catch
            {

            }

            return rpm;
        }

        //UnitLess VolumeMeasure
        private VolumeMeasure CreateVolMeasure(string val)
        {
            VolumeMeasure vm = new VolumeMeasure();
            try
            {
                vm.Value = Math.Round(Convert.ToDouble(val));
            }
            catch
            {

            }
            
            return vm;
        }

        private RefPositiveCount CreateRefPostiveCount(int num)
        {
            RefPositiveCount rc = new RefPositiveCount();
            try
            {
                rc.Value = (short)num;
            }
            catch
            {

            }

            return rc;
        }

        private RefNameString CreateRefNameString(string sName)
        {
            RefNameString rns = new RefNameString();
            try
            {
                rns.Value = sName;
            }
            catch
            {

            }

            return rns;
        }

        private AnglePerTimeMeasure CreateAnglePerTimeMeasure(double val, AnglePerTimeUom apt)
        {
            AnglePerTimeMeasure apm = new AnglePerTimeMeasure();
            try
            {
                apm.Uom = apt;
                apm.Value = val;
            }
            catch
            {

            }
            return apm;
        }

        private VolumeFlowRateMeasure CreateVolumeFlowRateMeasure(double val, VolumeFlowRateUom uom)
        {
            VolumeFlowRateMeasure vfr = new VolumeFlowRateMeasure();
            try
            {
                vfr.Uom = uom;
                vfr.Value = val;
            }
            catch
            {

            }
            return vfr;
        }

        private MassMeasure CreateMassMeasure(MassUom uom, double val)
        {
            MassMeasure mm = new MassMeasure();
            try
            {
                mm.Uom = uom;
                mm.Value = Math.Round(val, 2);
            }
            catch
            {


            }

            return mm;
        }

        private string GetSectionName()
        {
            string sectionName = string.Empty;
            using (var db = new EnerTraxDeployEntities(dbConn))
            {
                SectionName section = (from sn in db.SectionNames
                                       join se in db.Sections
                                       on sn.UIDSectionName equals se.IDSectionName
                                       where se.UIDSection == mudreport.IDSection
                                       select sn).First();
                sectionName = section.Name;
            }

            return sectionName;
        }

        private WellVerticalDepthCoord GetSectionDepth()
        {
            WellVerticalDepthCoord holeDepth=null;
            try
            {
                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    Section section = (from se in db.Sections
                                       where se.UIDSection == mudreport.IDSection
                                       select se).First();
                    holeDepth = CreateVerticalDepthCordMeasure((double)section.HoleDepth, WellVerticalCoordinateUom.m);
                }

            }
            catch
            {

            }
            
            return holeDepth;
        }

        private Timestamp CreateTimeStamp(DateTime dateVal)
        {
            DateTimeOffset dt = new DateTimeOffset(dateVal);          
            Timestamp ts = new Timestamp(dt.ToUniversalTime());
            
            return ts;            
        }

        private Timestamp CreateTimeStamp(DateTimeOffset dateVal)
        {
            Timestamp ts = new Timestamp(dateVal.ToUniversalTime());
            return ts;
        }

        private void WriteMudCheckValue(ref Fluid fluid, ref Rheometer rheometer, string mudType, string propertyName, string val)
        {
            try
            {
                switch (propertyName)
                {
                    case "Density":
                        fluid.Density = CreateDensityMeasure(val);
                        break;
                    case "ES Reading":
                        fluid.ElectStab = CreateElectricPotentialMeasure(val, ElectricPotentialUom.V);
                        break;
                    case "Filter Cake":
                        LengthMeasure lm = CreateLengthMeasure(val, LengthUom.mm);
                        if (mudType == "Invert")
                            fluid.FilterCakeLtlp = lm;
                        break;
                    case "HTHP":
                        fluid.FiltrateHthp = CreateVolMeasure(val, VolumeUom.mL);
                        break;
                    case "Fluid Loss @ 689.5 kPa":
                        fluid.FiltrateLtlp = CreateVolumeMeasureConverToM3PerDay(val);
                        break;
                    case "10 min. Gels":
                        fluid.Gel10Min = CreatePressureMeasure(val, PressureUom.Pa);
                        break;
                    case "10 sec. Gels":
                        fluid.Gel10Sec = CreatePressureMeasure(val, PressureUom.Pa);
                        break;
                    case "Excess Lime":
                        fluid.Lime = CreateDensityMeasure(val);
                        break;
                    case "MBT":
                        fluid.Mbt = CreateEquivalentPerMassMeasure(val, EquivalentPerMassUom.eqkg);
                        break;
                    case "Measured Depth":
                        fluid.MD = CreateMeasuredDepthCoordMeassure(val, MeasuredDepthUom.m);
                        break;
                    case "TVD Depth":
                        fluid.Tvd = CreateVerticalDepthCordMeasure(val, WellVerticalCoordinateUom.m);
                        break;
                    case "Mf Alkalinity":
                        fluid.Mf = CreateVolMeasure(val);
                        break;
                    case "Oil":
                    case "Oil (Retort)":
                        fluid.OilPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        break;
                    case "pH":
                        fluid.PH = Convert.ToDouble(val);
                        break;
                    case "Potassium":
                        fluid.Potassium = CreateDensityMeasure(val, DensityUom.mgL);
                        break;
                    case "Plastic Viscosity":
                        fluid.PV = CreateDynamicViscosityMeasure(val, DynamicViscosityUom.mPas);
                        break;
                    case "Sand":
                        fluid.SandPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        break;
                    case "Total Solids (Corrected)":
                        fluid.SolidsCalcPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        break;
                    case "High Gravity Solids OBM (%)":
                    case "High Gravity Solids (%)":
                        fluid.SolidsHiGravPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        break;
                    case "Low Gravity Solids OBM (%)":
                    case "Low Gravity Solids (%)":
                    case "Low Gravity Solids Enerclear (%)":
                        fluid.SolidsLowGravPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        break;
                    case "Total Solids":
                    case "Solids (Enerclear)":
                    case "Solids (Brine)":
                        fluid.SolidsPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        break;
                    case "Soluble Sulphides (GGT)":
                        fluid.Sulfide = CreateDensityMeasure(val, DensityUom.mgL);
                        break;
                    case "HTHP Temperature":
                        fluid.TempHthp = CreateThermodynamicTemperatureMeasure(val, ThermodynamicTemperatureUom.degC);
                        break;
                    case "Flowline Temperature":
                        ThermodynamicTemperatureMeasure ttm = CreateThermodynamicTemperatureMeasure(val, ThermodynamicTemperatureUom.degC);
                        fluid.TempPH = ttm;
                        fluid.TempVis = ttm;
                        break;
                    case "Viscosity":
                        fluid.VisFunnel = CreateTimeMeasure(val, TimeUom.s);
                        break;
                    case "Water":
                    case "Water (Retort)":
                        break;
                    case "Water (Enerclear)":
                        fluid.WaterPercent = CreateVolumePerVolumeMeasure(val, VolumePerVolumeUom.Item);
                        fluid.BrinePercent = fluid.WaterPercent;
                        break;
                    case "Yield Point":
                        fluid.YP = CreatePressureMeasure(val, PressureUom.Pa);
                        break;
                    case "Test Temperature (Fluid)":
                        rheometer.TempRheom = CreateThermodynamicTemperatureMeasure(val, ThermodynamicTemperatureUom.degC);
                        break;
                    case "100 RPM":
                        rheometer.Vis100Rpm = Convert.ToDouble(val);
                        break;
                    case "200 RPM":
                        rheometer.Vis200Rpm = Convert.ToDouble(val);
                        break;
                    case "300 RPM":
                        rheometer.Vis300Rpm = Convert.ToDouble(val);
                        break;
                    case "3 RPM":
                        rheometer.Vis3Rpm = Convert.ToDouble(val);
                        break;
                    case "600 RPM":
                        rheometer.Vis600Rpm = Convert.ToDouble(val);
                        break;
                    case "6 RPM":
                        rheometer.Vis6Rpm = Convert.ToDouble(val);
                        break;
                    case "Pf Alkalinity":
                        fluid.AlkalinityP1 = CreateVolMeasure(val);
                        break;
                    case "Calcium":
                    case "Calcium Water Phase":
                        fluid.Calcium = CreateDensityMeasure(val, DensityUom.mgL);
                        break;
                    case "Chlorides":
                    case "Chlorides Water Phase":
                        fluid.Chloride = CreateDensityMeasure(val, DensityUom.mgL);
                        break;
                    case "Time":
                        fluid.DateTime = GetMudCheckDate(val);
                        break;
                }
            }
            catch
            {

            }
        }

        private string GetMudCheckDataValue(List<MudCheckData> mcd, string PropertyName)
        {
            string val = string.Empty;
            try
            {
                MudCheckData mdValue = mcd.Where(x => x.MudCheckPropertyTypeJCT.MudCheckProperty.Name == PropertyName).FirstOrDefault();
                if (mdValue != null)
                    val = mdValue.Value;
            }
            catch
            {

            }

            return val;
        }

        private void RemoveMudCheckDataValue(List<MudCheckData> mcd, string PropertyName)
        {
            try
            {
                MudCheckData mdValue = mcd.Where(x => x.MudCheckPropertyTypeJCT.MudCheckProperty.Name == PropertyName).FirstOrDefault();
                if (mdValue != null)
                    mcd.Remove(mdValue);
            }
            catch
            {

            }
        }

        private void GenerateXMLFluidsReport(string directoryPath, string reportName)
        {           
            try
            {                                
                string xml = string.Empty;
                foreach (MudCheck mc in mudChecks)
                {                    
                    using (var db = new EnerTraxDeployEntities(dbConn))
                    {
                        List<MudCheckData> mcds = db.MudCheckDatas.Include("MudCheckPropertyTypeJCT").Where(x => x.IDMudCheck == mc.UIDMudCheck).ToList();
                        string mdVal = GetMudCheckDataValue(mcds, "Measured Depth");
                        string tvdVal = GetMudCheckDataValue(mcds, "TVD Depth");
                        string strMudCheckType = GetMudCheckType(db, mc.UIDMudCheck);
                        FluidsReport fr = CreateFluidsReport(mc, mdVal, tvdVal);
                        FluidsReportList fluidsReportList = new FluidsReportList();
                        fluidsReportList.FluidsReport = new List<FluidsReport>();
                        fluidsReportList.FluidsReport.Add(fr);
                        
                        List<Fluid> fluidlist = new List<Fluid>();
                        fr.Fluid = fluidlist;
                        Fluid fluid = new Fluid { };
                        fluid.Company = "Canadian Energy Services";
                        if (strMudCheckType == "EnerClear")
                            fluid.LocationSample = "Suction";
                        else
                            fluid.LocationSample = "Shakers";

                        if (!string.IsNullOrEmpty(ActiveFluidTech))
                            fluid.Engineer = ActiveFluidTech;

                        fluid.Uid = mc.UIDMudCheck.ToString();
                        fluid.MudClass = GetMudCheckType(mc.UIDMudCheck);
                        fluidlist.Add(fluid);
                        
                        // Rheometer
                        List<Rheometer> rheolist = new List<Rheometer>();
                        Rheometer rheometer = new Rheometer { };
                        rheometer.Uid = mc.MudcheckNo.ToString();
                        rheolist.Add(rheometer);
                        fluid.Rheometer = rheolist;

                        if (!string.IsNullOrEmpty(mdVal))
                            RemoveMudCheckDataValue(mcds, "Measured Depth");
                        if (!string.IsNullOrEmpty(tvdVal))
                            RemoveMudCheckDataValue(mcds, "TVD Depth");

                        fluid.Type = strMudCheckType;
                        foreach (MudCheckData mcd in mcds)                     
                            WriteMudCheckValue(ref fluid, ref rheometer, strMudCheckType, mcd.MudCheckPropertyTypeJCT.MudCheckProperty.Name, mcd.Value);
                        fluid.Comments = RemoveHTMLTags(mudreport.DailyActivity);

                        xml = EnergisticsConverter.ObjectToXml(fluidsReportList);
                        xml = ConvertDateTimeFormatToUniversalFormat(xml);
                        StreamWriter writer = new StreamWriter(directoryPath + "\\" + GetFileName(reportName, mc.MudcheckNo.ToString()));
                        writer.WriteLine(xml);
                        writer.Dispose();
                        writer.Close();
                    }
                }               
            }
            catch
            {

            }
        }

        private string RemoveHTMLTags(string strHTML)
        {
            string html = strHTML;
            try
            {              
                int openIndex = strHTML.IndexOf("<");
                int endIndex = strHTML.IndexOf(">");

                Regex rx = new Regex("<(.*?)>");
                html = rx.Replace(html, string.Empty);
            }
            catch
            {

            }

            return html;
        }

        private string GenerateXML(ReportType reportType)
        {
            // Ops Report
            OpsReportList opsreports = new OpsReportList();
            opsreports.OpsReport = new List<OpsReport>();

            OpsReport oreport = CreateOpsReport();               
            opsreports.OpsReport.Add(oreport);

            oreport.Fluid = GetMudCheckData(new List<Fluid>());
            oreport.MudInventory = MudInventoryUsageFill();
            oreport.PumpOperating = GetPumpData(ref oreport);
            oreport.ShakerOperating = GetShakerData(ref oreport);
            SetVolumeLosses(ref oreport);

            string xml = EnergisticsConverter.ObjectToXml(opsreports);
            xml = ConvertDateTimeFormatToUniversalFormat(xml);
            return xml;
        }

        private void SetVolumeLosses(ref OpsReport op)
        {
            try
            {
                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    op.MudVolume = new MudVolume();
                    VolumeControl vc = db.VolumeControls.Where(x => x.IDMudReport == mudreport.UIDMudReport).FirstOrDefault();
                    op.MudVolume.MudLosses = new MudLosses();

                    if(mudreport.Barite != null || mudreport.CalCarb != null)
                    {
                        double mudWeight = 0.0;
                        if (mudreport.Barite != null)
                        {
                            double? barite = mudreport.Barite;
                            double? bariteSG = mudreport.BariteSG;
                            double bariteM3 = 0.0;
                            if(bariteSG != 0 && barite != 0)
                                bariteM3 = (double)(barite / (1000 * bariteSG));

                            double? calCarb = mudreport.CalCarb;
                            double? calCarbSG = mudreport.CalCarbSG;
                            double calCarbM3 = 0.0;
                            if (calCarb != 0 && calCarbSG != 0)
                                calCarbM3 = (double)(calCarb / (1000 * calCarbSG));

                            mudWeight += bariteM3 + calCarbM3;
                            if(mudWeight > 0)
                                op.MudVolume.VolMudBuilt = CreateVolMeasure(mudWeight.ToString(), VolumeUom.m3);
                        }
                    }
                    
                    if (vc.Shaker != null)
                        op.MudVolume.MudLosses.VolLostShakerSurf = CreateVolMeasure(vc.Shaker.ToString(), VolumeUom.m3);

                    if (vc.SeepageLosses != null)
                        op.MudVolume.MudLosses.VolLostCircHole = CreateVolMeasure(vc.SeepageLosses.ToString(), VolumeUom.m3);

                    if (vc.Centrifuge != null)
                        op.MudVolume.MudLosses.VolLostMudCleanerSurf = CreateVolMeasure(vc.Centrifuge.ToString(), VolumeUom.m3);

                    if (vc.SurfaceLosses != null)
                        op.MudVolume.MudLosses.VolLostOtherSurf = CreateVolMeasure(vc.SurfaceLosses.ToString(), VolumeUom.m3);

                    if (vc.MudLosses != null)
                        op.MudVolume.MudLosses.VolLostOtherHole = CreateVolMeasure(vc.MudLosses.ToString(), VolumeUom.m3);
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        //private void GetOBMVolume(EnerTraxDeployEntities db, ref OpsReport opReport)
        //{
        //    List<ProductTransactionJCT> delivOBM = null;
        //    List<ProductTransactionJCT> creditOBM = null;
        //    List<ProductWellUsageJCT> usageOBM = null;
        //    try
        //    {
        //        delivOBM = (from t in db.ProductTransactions
        //                    join tJCT in db.ProductTransactionJCTs on t.UIDProductTransactions equals tJCT.IDProductTransaction
        //                    join tlTo in db.TransactionLocations on t.IDTransactionLocationTo equals tlTo.UIDTransactionLocation
        //                    join p in db.Products on tJCT.IDProduct equals p.UIDProduct
        //                    where tlTo.IDLocation == well.UIDWell
        //                           && (p.ProductName.Contains("Invert") || p.ProductName.Contains("Base Oil")
        //                           && t.TransactionDate == mudreport.MRDate)
        //                    select tJCT
        //                     ).Include("ProductTransaction").ToList();

        //        creditOBM = (from t in db.ProductTransactions
        //                     join tJCT in db.ProductTransactionJCTs on t.UIDProductTransactions equals tJCT.IDProductTransaction
        //                     join tlTo in db.TransactionLocations on t.IDTransactionLocationTo equals tlTo.UIDTransactionLocation
        //                     join p in db.Products on tJCT.IDProduct equals p.UIDProduct
        //                     where tlTo.IDLocation == well.UIDWell && (p.ProductName.Contains("Invert") || p.ProductName.Contains("Base Oil"))
        //                     select tJCT
        //                    ).Include("ProductTransaction").ToList();

        //        usageOBM = (from u in db.ProductUsages
        //                    join uJCT in db.ProductWellUsageJCTs on u.UIDProductUsage equals uJCT.IDProductUsage
        //                    join se in db.Sections on u.IDSection equals se.UIDSection
        //                    join wp in db.WellProducts on uJCT.IDWellProducts equals wp.UIDWellProducts
        //                    where se.IDWell == well.UIDWell
        //                    select uJCT
        //                    ).Include("ProductUsage").ToList();
        //    }
        //    catch
        //    {

        //    }
        //}

        private void GetInventory(EnerTraxDeployEntities db, ref OpsReport opReport)
        {
            List<ProductTransactionJCT> deliv = null;
            List<ProductTransactionJCT> credit = null;
            List<ProductWellUsageJCT> usage = null;
            try
            {
                deliv = (from t in db.ProductTransactions
                            join tJCT in db.ProductTransactionJCTs on t.UIDProductTransactions equals tJCT.IDProductTransaction
                            join tlTo in db.TransactionLocations on t.IDTransactionLocationTo equals tlTo.UIDTransactionLocation
                            join p in db.Products on tJCT.IDProduct equals p.UIDProduct
                            where tlTo.IDLocation == well.UIDWell
                                   && t.TransactionDate >= mudreport.InventoryStartDate
                                   && t.TransactionDate <= mudreport.InventoryEndDate
                            select tJCT
                             ).Include("ProductTransaction").ToList();

                credit = (from t in db.ProductTransactions
                             join tJCT in db.ProductTransactionJCTs on t.UIDProductTransactions equals tJCT.IDProductTransaction
                             join tlTo in db.TransactionLocations on t.IDTransactionLocationTo equals tlTo.UIDTransactionLocation
                             join p in db.Products on tJCT.IDProduct equals p.UIDProduct
                             where tlTo.IDLocation == well.UIDWell 
                                    && t.TransactionDate >= mudreport.InventoryStartDate
                                    && t.TransactionDate <= mudreport.InventoryEndDate
                             select tJCT
                            ).Include("ProductTransaction").ToList();

                usage = (from u in db.ProductUsages
                            join uJCT in db.ProductWellUsageJCTs on u.UIDProductUsage equals uJCT.IDProductUsage
                            join se in db.Sections on u.IDSection equals se.UIDSection
                            join wp in db.WellProducts on uJCT.IDWellProducts equals wp.UIDWellProducts
                            where se.IDWell == well.UIDWell
                                    && u.UsageDate >= mudreport.InventoryStartDate
                                    && u.UsageDate <= mudreport.InventoryEndDate
                            select uJCT
                            ).Include("ProductUsage").ToList();
            }
            catch
            {

            }
        }

        private MudLosses CreateMudLosses(VolumeControl vc)
        {
            MudLosses ml = new MudLosses();
            try
            {              
                if (vc.MudLosses != null)
                    ml.VolLostOtherHole = CreateVolMeasure(vc.MudLosses.ToString(), VolumeUom.m3);
                if(vc.MudLosses != null || vc.SeepageLosses != null)
                {
                    double? totalLossToHole = 0.0;
                    if (vc.MudLosses != null)
                        totalLossToHole += vc.MudLosses;
                    if (vc.SeepageLosses != null)
                        totalLossToHole += vc.SeepageLosses;

                    ml.VolTotMudLostHole = CreateVolMeasure(totalLossToHole.ToString(), VolumeUom.m3);
                }
            }
            catch
            {

            }

            return ml;
        }

        private double? GetTotalSurfaceLosses(VolumeControl vc)
        {
            double? totalSurfLosses = null;
            try
            {
                
                if (vc.Shaker != null || vc.Centrifuge != null || vc.SurfaceLosses != null)
                {
                    totalSurfLosses = 0;
                    if (vc.Shaker != null)
                        totalSurfLosses += vc.Shaker;
                    if (vc.Centrifuge != null)
                        totalSurfLosses += vc.Centrifuge;
                    if (vc.SurfaceLosses != null)
                        totalSurfLosses += vc.SurfaceLosses;
                }
            }
            catch
            {

            }
            return totalSurfLosses;
        }

        private List<ShakerOp> GetShakerData(ref OpsReport oReport)
        {
            List<ShakerOp> shakers = new List<ShakerOp>();
            try
            {
                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    List<Shaker> sList = (from s in db.Shakers
                                          where s.IDMudReport == mudreport.UIDMudReport
                                          select s).ToList();

                    foreach(Shaker s in sList)
                    {
                        ShakerOp shaker = new ShakerOp();
                        shaker.Uid = s.UIDShaker.ToString();
                        shaker.Shaker = CreateRefNameString(s.Type);
                        shaker.DateTime = CreateTimeStamp((DateTime)mudreport.MRDate);
                    
                        if (oReport.Fluid != null)
                        {
                            List<Fluid> fluids = oReport.Fluid;
                            Fluid fluid = fluids.LastOrDefault();
                            if (fluid.MD != null)                           
                                shaker.MDHole = fluid.MD;                            
                        }
                        shakers.Add(shaker);
                    }

                }
            }
            catch
            {

            }
            return shakers;
        }

        private List<PumpOp> GetPumpData(ref OpsReport oReport)
        {
            List<PumpOp> pumps = new List<PumpOp>();
            try
            {
                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    List<Pump> pList = (from p in db.Pumps
                                 where p.IDMudReport == mudreport.UIDMudReport
                                 select p).ToList();

                    foreach(Pump p in pList)
                    {
                        PumpOp pump = new PumpOp();
                        pump.Uid = p.UIDPump.ToString();
                        pump.Pump = CreateRefPostiveCount((int)p.Sequence);
                        pump.DateTime = CreateTimeStamp((DateTime)mudreport.MRDate);
                        if(p.LinerSize != null)
                            pump.LinerSize = CreateLengthMeasure(p.LinerSize.ToString(), LengthUom.mm);
                        if (p.StrokeLength != null)
                            pump.LenStroke = CreateLengthMeasure(p.StrokeLength.ToString(), LengthUom.mm);
                        if (p.PumpSpeed != null)
                            pump.RateStroke = CreateAnglePerTimeMeasure((double)p.PumpSpeed, AnglePerTimeUom.rpm);
                        if (p.Pressure != null)
                            pump.Pressure = CreatePressureMeasure(p.Pressure.ToString(), PressureUom.kPa);
                        if (p.Efficiency != null)
                            pump.PercentEfficiency = CreateRelativePowerMeasure((double)p.Efficiency, RelativePowerUom.Item);
                        if (p.Efficiency != null && p.LinerSize != null && p.StrokeLength != null && p.PumpSpeed != null)
                        {
                            double? pOutput = CalculatePumpOutput((double)p.Efficiency, (double)p.LinerSize, (double)p.StrokeLength, (double)p.PumpSpeed);
                            pump.PumpOutput = CreateVolumeFlowRateMeasure((double)pOutput, VolumeFlowRateUom.m3min);
                        }
                        if(oReport.Fluid != null)
                        {
                            List<Fluid> fluids = oReport.Fluid;
                            Fluid fluid = fluids.LastOrDefault();
                            if(fluid.MD != null)
                            {
                                pump.MDBit = fluid.MD;
                            }
                        }
                        pumps.Add(pump);
                    }                   
                }                    
            }
            catch
            {

            }
            return pumps;
        }

        private double? CalculatePumpOutput(double efficiency, double linerSize, double strokeLength, double pumpSpeed)
        {
            double? outputVal = null;
            try
            {
                outputVal = (((efficiency / 100) * 3 * Math.PI * 0.25 * Math.Pow(linerSize, 2) * strokeLength) / 1000000000) * pumpSpeed;
                outputVal = Math.Round((double)outputVal, 2);
            }
            catch
            {

            }

            return outputVal;
        }

        private OpsReport CreateOpsReport()
        {
            OpsReport oreport = new OpsReport { };           
            oreport.NameWell = well.WellName;
            oreport.UidWell = well.UIDWell.ToString();
            oreport.Uid = "UIDWell";
            oreport.UidWellbore = mudreport.IDSection.ToString();
            oreport.NameWellbore = GetSectionName();
            oreport.Name = well.WellName;
            oreport.DateTime = CreateTimeStamp((DateTime)mudreport.MRDate);

            return oreport;
        }

        private FluidsReport CreateFluidsReport(MudCheck mc, string mdVal, string tvdVal)
        {
            FluidsReport fReport = new FluidsReport();
            try
            {
                fReport.UidWell = well.UIDWell.ToString();
                fReport.Uid = well.UIDWell.ToString();
                fReport.UidWellbore = mudreport.IDSection.ToString();
                fReport.NameWell = well.WellName;
                fReport.NameWellbore = GetSectionName();
                fReport.Name = well.WellName;
                fReport.DateTime = CreateTimeStamp((DateTime)mudreport.MRDate);
                fReport.MD = CreateMeasuredDepthCoordMeassure(mdVal, MeasuredDepthUom.m);
                fReport.Tvd = CreateVerticalDepthCordMeasure(tvdVal, WellVerticalCoordinateUom.m);
                fReport.NumReport = (short)mc.MudcheckNo;                
            }
            catch
            {

            }

            return fReport;
        }

        //defaults UOM to Kg/m3
        private DensityMeasure CreateDensityMeasure(string val)
        {
            DensityMeasure dm = new DensityMeasure();

            try
            {
                dm.Uom = DensityUom.kgm3;
                dm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
            return dm;
        }

        private DensityMeasure CreateDensityMeasure(string val, DensityUom uom)
        {
            DensityMeasure dm = new DensityMeasure();
            try
            {
                dm.Uom = uom;
                dm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
            return dm;
        }

        private ElectricPotentialMeasure CreateElectricPotentialMeasure(string val, ElectricPotentialUom uom)
        {
            ElectricPotentialMeasure es = new ElectricPotentialMeasure();
            try
            {
                es.Uom = uom;
                es.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
            return es;
        }

        private LengthMeasure CreateLengthMeasure(string val, LengthUom uom)
        {
            LengthMeasure lm = new LengthMeasure();
            try
            {
               
                lm.Uom = uom;
                lm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
            return lm;
        }

        private VolumeMeasure CreateVolumeMeasureConverToM3PerDay(string val)
        {
            VolumeMeasure vm = new VolumeMeasure();
            try
            {
                vm.Uom = VolumeUom.m3;
                vm.Value = Convert.ToDouble(val) * 4.8 * Math.Pow(10, -5);
                vm.Value = Math.Round(vm.Value, 2);

            }
            catch
            {

            }
            
            return vm;
        }

        private PressureMeasure CreatePressureMeasure(string val, PressureUom uom)
        {
            PressureMeasure pm = new PressureMeasure();
            try
            {
                pm.Uom = uom;
                pm.Value = Convert.ToDouble(val);
            }
            catch
            {

            }
            
            return pm;
        }

        private EquivalentPerMassMeasure CreateEquivalentPerMassMeasure(string val, EquivalentPerMassUom uom)
        {
            EquivalentPerMassMeasure epm = new EquivalentPerMassMeasure();
            try
            {
                epm.Uom = uom;
                epm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
            
            return epm;
        }

        private MeasuredDepthCoord CreateMeasuredDepthCoordMeassure(string val, MeasuredDepthUom uom)
        {
            MeasuredDepthCoord mdc = new MeasuredDepthCoord();
            try
            {
                mdc.Uom = uom;
                mdc.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }

            return mdc;
        }



        private WellVerticalDepthCoord CreateVerticalDepthCordMeasure(double val, WellVerticalCoordinateUom uom)
        {
            WellVerticalDepthCoord wvd = new WellVerticalDepthCoord();
            try
            {
                wvd.Uom = uom;
                wvd.Value = Math.Round(val, 2);
            }
            catch
            {

            }

            return wvd;
        }

        private WellVerticalDepthCoord CreateVerticalDepthCordMeasure(string val, WellVerticalCoordinateUom uom)
        {
            WellVerticalDepthCoord wvd = new WellVerticalDepthCoord();
            try
            {
                wvd.Uom = uom;
                wvd.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }

            return wvd;
        }

        private VolumePerVolumeMeasure CreateVolumePerVolumeMeasure(string val, VolumePerVolumeUom uom)
        {
            VolumePerVolumeMeasure vpvm = new VolumePerVolumeMeasure();
            try
            {
                vpvm.Uom = uom;
                vpvm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }

            return vpvm;
        }

        private DynamicViscosityMeasure CreateDynamicViscosityMeasure(string val, DynamicViscosityUom uom)
        {
            DynamicViscosityMeasure dvm = new DynamicViscosityMeasure();
            try
            {
                dvm.Uom = uom;
                dvm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }
            return dvm;
        }

        private ThermodynamicTemperatureMeasure CreateThermodynamicTemperatureMeasure(string val, ThermodynamicTemperatureUom uom)
        {
            ThermodynamicTemperatureMeasure ttm = new ThermodynamicTemperatureMeasure();
            try
            {
                ttm.Uom = uom;
                ttm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }

            return ttm;
        }

        private TimeMeasure CreateTimeMeasure(string val, TimeUom uom)
        {
            TimeMeasure tm = new TimeMeasure();
            try
            {
                tm.Uom = uom;
                tm.Value = Math.Round(Convert.ToDouble(val), 2);
            }
            catch
            {

            }

            return tm;
        }

        private string ConvertDateTimeFormatToUniversalFormat(string xml)
        {
            try
            {
                Regex rx = new Regex(@"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{7})\+(\d{2}):(\d{2})");
                foreach (Match m in rx.Matches(xml))
                {
                    xml = xml.Replace(m.Value, m.Value.Substring(0, 19) + "Z");
                }

            }
            catch
            {

            }

            return xml;
        }

        private Timestamp GetMudCheckDate(string val)
        {
            Timestamp ts = new Timestamp();
            try
            {
                DateTimeOffset dtTime;
                DateTimeOffset.TryParse(val, out dtTime);
                DateTime mrDate = (DateTime)mudreport.MRDate;
                DateTimeOffset dtof = mrDate.ToLocalTime();

                DateTimeOffset dtVal = new DateTimeOffset(mrDate.Year, mrDate.Month, mrDate.Day, dtTime.Hour, dtTime.Minute, dtTime.Second, dtof.Offset);

                ts = CreateTimeStamp(dtVal);
            }
            catch
            {

            }

            return ts;
        }

        private string GetMudCheckType(EnerTraxDeployEntities db, Guid IDMudCheck)
        {
            string mcType = string.Empty;
            mcType = (from mcd in db.MudCheckDatas
                      join mcJCT in db.MudCheckPropertyTypeJCTs on mcd.IDMudCheckPropertyTypeJCT equals mcJCT.UIDMudCheckPropertyTypeJCT
                      join mct in db.MudCheckTypes on mcJCT.IDMudCheckType equals mct.UIDMudCheckType
                      where mcd.IDMudCheck == IDMudCheck
                      select mct.Name).FirstOrDefault();

            return mcType;
        }

        private MudClass GetMudCheckType(Guid IDMudCheck)
        {
            string strMudCheckProperty = string.Empty;
            MudClass mc = new MudClass();
            try
            {
                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    strMudCheckProperty = GetMudCheckType(db, IDMudCheck);
                }

                switch (strMudCheckProperty)
                {
                    case "OBM":
                        mc = MudClass.oilbased;
                        break;
                    case "WBM":
                    case "Enerclear":
                        mc = MudClass.waterbased;                        
                        break;
                    default:
                        mc = MudClass.other;
                        break;
                }

                }
            catch
            {

            }
            return mc;
        }
        

        // Get Mud Check Data
        private List<Fluid> GetMudCheckData(List<Fluid> fluidlist)
        {
            foreach(MudCheck mc in mudChecks)
            {
                Fluid fluid = new Fluid { };
                fluid.Uid = mc.UIDMudCheck.ToString();
                fluid.MudClass = GetMudCheckType(mc.UIDMudCheck); 
                fluidlist.Add(fluid);

                // Rheometer
                List<Rheometer> rheolist = new List<Rheometer>();
                Rheometer rheometer = new Rheometer { };
                rheometer.Uid = mc.MudcheckNo.ToString();
                rheolist.Add(rheometer);
                fluid.Rheometer = rheolist;

                using (var db = new EnerTraxDeployEntities(dbConn))
                {
                    var query = (from mcd in db.MudCheckDatas
                                 join mcpt in db.MudCheckPropertyTypeJCTs
                                    on mcd.IDMudCheckPropertyTypeJCT equals mcpt.UIDMudCheckPropertyTypeJCT
                                 join mcp in db.MudCheckProperties
                                    on mcpt.IDMudCheckProperty equals mcp.UIDMudCheckProperty
                                 join mct in db.MudCheckTypes
                                    on mcpt.IDMudCheckType equals mct.UIDMudCheckType
                                 join mc2 in db.MudChecks
                                    on mcd.IDMudCheck equals mc2.UIDMudCheck
                                 join mr in db.MudReports
                                    on mc2.IDMudReport equals mr.UIDMudReport
                                 join mt in db.MudTypes
                                    on mr.IDMudType equals mt.UIDMudType
                                 where mcd.IDMudCheck == mc.UIDMudCheck
                                 select new { Val = mcd.Value, Propertyname = mcp.Name, MudCheckType = mct.Name, MudType = mt.Name }).ToList();

                    foreach (var row in query)
                    {
                        if (!(String.IsNullOrEmpty(row.Val) || String.IsNullOrEmpty(row.Propertyname)))
                            // Mapping for mudType
                            //public enum MudClass
                            //{
                            //    waterbased = 0,
                            //    oilbased = 1,
                            //    other = 2,
                            //    pneumatic = 3,
                            //    unknown = 4
                            //}

                            if (row.MudType.Contains("OBM"))
                                fluid.Type = "Reservoir NAF or(OBM)";
                            else if (row.MudType == "Brine")
                                fluid.Type = row.MudType;
                            else
                                fluid.Type = "Water Based";

                            WriteMudCheckValue(ref fluid, ref rheometer, row.MudType, row.Propertyname, row.Val);
                    }
                }
            }

            if(fluidlist.Count > 0)
            {
                Fluid f = fluidlist.LastOrDefault();
                f.Comments = RemoveHTMLTags(mudreport.DailyActivity);
            }

            return fluidlist;
        }        
    }
}
