# -*- coding: cp1252 -*-
__author__ = "Jean-Francois N'GUYEN VAN-SOC - OEIL"
__copyright__ = "Copyright 2021, OEIL"
__credits__ = ["Jean-François N'GUYEN VAN-SOC - OEIL"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Jean-Francois N'GUYEN VAN-SOC"
__email__ = "jf.nguyenvansoc@oeil.nc"
__status__ = "Developpement"

import os, datetime, calendar, json, arcpy, ftplib, smtplib, sys, subprocess
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
arcpy.env.parallelProcessingFactor = "80%"

nomscript = sys.argv[0].replace("\\","/")
nomscript = nomscript.split("/")
nomscript = nomscript[len(nomscript)-1][:-3]
try:
    try:
        rep = os.getcwd().replace("\\","/")
        replog = rep + "/log"
        flog = replog+ "/" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "_" + nomscript + ".txt"
        log = open(flog,"w")
        print("debut du processus: " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        log.write("debut du processus: " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S") +"\n")
        #  Address: ftp.ptree.jaxa.jp
        # UID: fabien.albouy_oeil.nc
        # PW: SP+wari8
        arcpy.AddMessage("Declarations des fonctions necessaires au traitement")
        js = open(rep + "/Config.json","r")
        conf = json.load(js)
        FromADDR = conf["fromaddr"]
        ToADDR = conf["toaddr"]
        ToADDR2 = conf["toaddr2"]
        SMTP = conf["SMTP"]
        SMTPPort = conf["SMTPPort"]
        CompteEmail = conf["CompteEmail"]
        MDPEmail = conf["MDPEmail"]
        CommandeFME = conf["CommandeFME"]
        js.close()
        del conf
        #######################################FONCTIONS###################################################################
        def PrintEtLog(txt):
            print(txt)
            log.write(txt + "\n")

        ###############Fonction de verification pour le traitement entier d'un repertoire Mois#########################
        def VerificationTraitementMois(rMois,joursbd):
            mois30 = ['04', '06', '09', '11']
            mois31 = ['01', '03', '05', '07', '08', '10', '12']
            JDBNoms = [j[0] for j in joursbd]
            pMois = rMois[-2:]
            annee = int(rMois[:4])
            #les mois à 30 jours
            if pMois in mois30:
                if '30' in JDBNoms:
                    Jour30DBFiltre = [jdb for jdb in joursbd if jdb[0] == '30']
                    Jour30DBFiltre = Jour30DBFiltre[0]
                    if Jour30DBFiltre[3] == 1:
                        MoisTraite = 1
                    else:
                        MoisTraite = 0
                else:
                    MoisTraite = 0
            #les mois a 31 jours
            elif pMois in mois31:
                if '31' in JDBNoms:
                    Jour31DBFiltre = [jdb for jdb in joursbd if jdb[0] == '31']
                    Jour31DBFiltre = Jour31DBFiltre[0]
                    if Jour31DBFiltre[3] == 1:
                        MoisTraite = 1
                    else:
                        MoisTraite = 0
                else:
                    MoisTraite = 0
            #mois de Fevrier et annee bisextile
            elif pMois == '02':
                if calendar.isleap(annee):
                    if '29' in JDBNoms:
                        Jour29DBFiltre = [jdb for jdb in joursbd if jdb[0] == '29']
                        Jour29DBFiltre = Jour29DBFiltre[0]
                        if Jour29DBFiltre[3] == 1:
                            MoisTraite = 1
                        else:
                            MoisTraite = 0
                    else:
                        MoisTraite = 0
                else:
                    if '28' in JDBNoms:
                        Jour28DBFiltre = [jdb for jdb in joursbd if jdb[0] == '28']
                        Jour28DBFiltre = Jour28DBFiltre[0]
                        if Jour28DBFiltre[3] == 1:
                            MoisTraite = 1
                        else:
                            MoisTraite = 0
                    else:
                        MoisTraite = 0
            return MoisTraite

        ###Fonction de correction du fichier CSV#### FONCTION OBSOLETE plus besoin de formater
        def FormatageCSV(rprt, CSVFile):
            LienCSV = rprt + "/" + CSVFile
            LienNewCSV = LienCSV.replace(".csv", "_corrige.csv")
            if os.path.isfile(LienNewCSV):
                # si le csv corrige existe deja. Faire un autre script de reinitialisation.
                PrintEtLog("fichier " + LienNewCSV + " existe deja")
            else:
                PrintEtLog("formatage et correction du fichier csv " + CSVFile + " dans " + rprt)
                Csv = open(LienCSV, "rb")
                NCsv = open(LienNewCSV, "wb")
                Lines = Csv.readlines()
                NLines = list()
                for l in Lines:
                    # on recherche le motif MUTIPOLYGON s'il est present on traite differemment
                    m = l.find("MULTIPOLYGON")
                    if m >= 0:
                        lp1 = l[0:m].replace(",", ";")
                        lp2 = l[m:len(l)].replace(",", "],[")
                        lp2 = lp2.replace("(((", "[[[").replace(")))", "]]]}").replace("MULTIPOLYGON", "{\"rings\":")
                        lp2 = lp2.replace("   ", ",")
                        lp2 = lp2.replace("  ", ",")
                        lp2 = lp2.replace(" ", "")
                        lp2 = lp2.replace("[,", "[")
                        nl = lp1 + lp2
                        nl = nl.replace("'", "")
                        NLines.append(nl)
                    else:
                        nl = l.replace(",", ";")
                        nl = nl.replace("'", "")
                        NLines.append(nl)
                for nl in NLines:
                    NCsv.write(nl)
                Csv.close()
                NCsv.close()
            return CSVFile.replace(".csv","_corrige.csv")

        ###############Fonction pour envoi de message pour la donnee Himawari concernant la NC et completer Table des CSV#####################################################
        def HimawariEmailTableCSV(CSVFile, ListeCodeVerif, TableDesCSV, ChampsDesCSV, idr):
            CSVFileOrig = CSVFile.replace("_corrige","")
            if len(ListeCodeVerif) == 0:
                #PrintEtLog("Code 0: Aucune donnee pour la NC sur " + CSVFileOrig)
                iCursCSV = arcpy.da.InsertCursor(TableDesCSV, ChampsDesCSV)
                rowCSV = [CSVFileOrig, 0, idr]
                iCursCSV.insertRow(rowCSV)
                del iCursCSV
            elif 5 in ListeCodeVerif:
                if 1 in ListeCodeVerif:
                    messagePrint = "Code 5: Donnees trouvees pour la NC sur " + CSVFileOrig + " et seront traitees. Des lignes ne sont pas traitables." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie et vont etre traites. Des lignes ne sont pas traitables."
                else:
                    messagePrint = "Code 5: Donnees trouvees pour la NC sur " + CSVFileOrig + " et seront traitees." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie et vont etre traites."
                objet = conf["SubjectMailSuccess"]
                iCursCSV = arcpy.da.InsertCursor(TableDesCSV, ChampsDesCSV)
                rowCSV = [CSVFileOrig, 5, idr]
                iCursCSV.insertRow(rowCSV)
                del iCursCSV
                ####### Lancement de la commande FME si code 5
                subprocess.Popen(CommandeFME,cwd="D:/ALERTE_INCENDIE_ENVIRONNEMENTALE/NC/Production/Firms/02_FME")
            elif 4 in ListeCodeVerif:
                if 1 in ListeCodeVerif:
                    messagePrint = "Code 4: Donnees trouvees pour la NC sur " + CSVFileOrig + " mais la superficie de la surface est en dehors du perimetre ou inferieur a 20ha. Des lignes ne sont pas traitables." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie mais la superficie de la surface est en dehors du perimetre ou inferieur a 20ha. Des lignes ne sont pas traitables."
                else:
                    messagePrint = "Code 4: Donnees trouvees pour la NC sur " + CSVFileOrig + " mais la superficie de la surface est en dehors du perimetre ou inferieur a 20ha." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie mais la superficie de la surface est en dehors du perimetre ou inferieur a 20ha."
                objet = conf["SubjectMail"]
                iCursCSV = arcpy.da.InsertCursor(TableDesCSV, ChampsDesCSV)
                rowCSV = [CSVFileOrig, 4, idr]
                iCursCSV.insertRow(rowCSV)
                del iCursCSV
            elif 3 in ListeCodeVerif:
                if 1 in ListeCodeVerif:
                    messagePrint = "Code 3: Donnees trouvees pour la NC sur " + CSVFileOrig + " mais ont ete elaguees par rapport au seuil horaire. Des lignes ne sont pas traitables." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie mais a ete filtre par rapport au seuil horaire. Des lignes ne sont pas traitables."
                else:
                    messagePrint = "Code 3: Donnees trouvees pour la NC sur " + CSVFileOrig + " mais ont ete elaguees par rapport au seuil horaire."
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie mais a ete filtre par rapport au seuil horaire."
                objet = conf["SubjectMail"]
                iCursCSV = arcpy.da.InsertCursor(TableDesCSV, ChampsDesCSV)
                rowCSV = [CSVFileOrig, 3, idr]
                iCursCSV.insertRow(rowCSV)
                del iCursCSV
            elif 2 in ListeCodeVerif:
                if 1 in ListeCodeVerif:
                    messagePrint = "Code 2: Donnees trouvees pour la NC sur " + CSVFileOrig + " mais ont ete elaguees par rapport au perimetre NC. Des lignes ne sont pas traitables." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie mais a ete filtre par rapport au perimetre. Des lignes ne sont pas traitables."
                else:
                    messagePrint = "Code 2: Donnees trouvees pour la NC sur " + CSVFileOrig + " mais ont ete elaguees par rapport au perimetre NC." + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    body = "le fichier " + CSVFileOrig + " contient des points d'anomalies thermiques sur la Nouvelle-Caledonie mais a ete filtre par rapport au perimetre."
                objet = conf["SubjectMail"]
                iCursCSV = arcpy.da.InsertCursor(TableDesCSV, ChampsDesCSV)
                rowCSV = [CSVFileOrig, 2, idr]
                iCursCSV.insertRow(rowCSV)
                del iCursCSV
            elif 0 in ListeCodeVerif:
                #print("Code 0: Aucune donnee pour la NC sur " + CSVFile)
                iCursCSV = arcpy.da.InsertCursor(TableDesCSV, ChampsDesCSV)
                rowCSV = [CSVFileOrig, 0, idr]
                iCursCSV.insertRow(rowCSV)
                del iCursCSV

            if len(ListeCodeVerif) > 0:
                if 5 in ListeCodeVerif:
                    PrintEtLog(messagePrint)
                    #envoi email vers jf.nguyenvansoc@oeil.nc et fabien.albouy@oeil.nc
                    fromaddr = FromADDR
                    toaddr = ToADDR
                    toaddr2 = ToADDR2
                    msg = MIMEMultipart()
                    msg['Subject'] = objet
                    msg.attach(MIMEText(body, 'plain'))
                    server = smtplib.SMTP(SMTP, SMTPPort)
                    server.starttls()
                    server.login(CompteEmail, MDPEmail)
                    text = msg.as_string()
                    server.sendmail(fromaddr, toaddr, text)
                    #server.sendmail(fromaddr, toaddr2, text)
                    server.quit()

        ###############Fonction pour creer la surface (pixel 2km de cote) a partir du point#####################################################
        def PointToGeom(PtGeom,proj):
            x = PtGeom[0].X
            y = PtGeom[0].Y
            x1 = x - 1000
            y1 = y - 1000
            x2 = x + 1000
            y2 = y + 1000
            polygone = arcpy.Array([arcpy.Point(x1,y1),arcpy.Point(x1,y2),arcpy.Point(x2,y2),arcpy.Point(x2,y1)])
            PolygGeom = arcpy.Polygon(polygone)
            PolygGeom = PolygGeom.projectAs(proj)
            return PolygGeom

        ###############Fonction de transformation en donnee geographique puis verification si dans contour et seuilHoraire#########################
        ###Recommandation les couches restent maintenant a la projection2 ex: RGNC et non WGS1984 #######
        def RecupDonneesCSV(rprt, CSVFile, NomCouchePts, NomCouchePol, GdbCouche, ChampsCouche, Etendue, TableCSV, ChampTableCSV, contour, contourBuf, cdprj1, cdprj2, SeuilH, idrepHeure, LienSDE, transfprj=""):
            CSV = open(rprt + "/" + CSVFile, "rb")
            Lines = CSV.readlines()
            CSV.close()
            #recuperation des geometries de la couche de filtre NC
            repRac = rprt.replace("/Download", "")
            arcpy.MakeFeatureLayer_management(repRac + "/" +  GdbCouche + "/" + contourBuf, contourBuf)
            GeomsContourBuff = [cb[0] for cb in arcpy.da.SearchCursor(contourBuf,["SHAPE@"])]
            arcpy.Delete_management(contourBuf)
            arcpy.MakeFeatureLayer_management(repRac + "/" + GdbCouche + "/" + contour, contour)
            GeomsContour = [c[0] for c in arcpy.da.SearchCursor(contour, ["SHAPE@"])]
            arcpy.Delete_management(contour)
            minlon = float(Etendue[0])
            maxlon = float(Etendue[2])
            minlat = float(Etendue[1])
            maxlat = float(Etendue[3])
            prj1 = arcpy.SpatialReference(cdprj1)
            prj2 = arcpy.SpatialReference(cdprj2)
            Verif = list()
            for l in Lines:
                i = Lines.index(l)
                rowpol = list()
                rowpt = list()
                dl = l.replace("'","").split(",")
                verifline = 0
                #si les informations sont completes
                if i > 1 and len(dl) >= 14:
                    lon = float(dl[6])
                    lat = float(dl[5])
                    #premiere verification si c'est dans la zone NC globale (Etendue)
                    if lon > minlon and lon < maxlon and lat > minlat and lat < maxlat:
                        verifline = 2
                        p = arcpy.Point(lon, lat)
                        pt = arcpy.PointGeometry(p, prj1)
                        if transfprj == "":
                            pt_prj2 = pt.projectAs(prj2)
                        else:
                            pt_prj2 = pt.projectAs(prj2, transfprj)
                        #deuxieme verification: si le point est a l'interieur de chaque geometrie de la couche Contour avec Tampon 2KM
                        for gcb in GeomsContourBuff:
                            if pt_prj2.within(gcb):
                                verifline = 3
                        if verifline == 3:
                            #if lon > minlon and lon < maxlon and lat > minlat and lat < maxlat:
                                #Verif = 1
                            y = int(dl[1])
                            m = int(dl[2])
                            d = int(dl[3])
                            H = int(dl[4][0:2])
                            M = int(dl[4][2:4])
                            obstime = datetime.datetime(y,m,d,H,M,0)
                            obstimeGMT11 = obstime + datetime.timedelta(hours=11)  # obs timeGMT11
                            Sql_obstimeGMT11 = obstimeGMT11.strftime("%Y-%m-%d %H:%M:%S")
                            #date moins le seuil horaire
                            obstimeGMT11SeuilH = obstimeGMT11 - datetime.timedelta(hours=SeuilH)
                            Sql_obstimeGMT11SeuilH = obstimeGMT11SeuilH.strftime("%Y-%m-%d %H:%M:%S")
                            # on constitue une requete SQL pour respecter les seuils
                            SqlFiltreSeuilH = ChampsCouche[13] + " = " + dl[6] + " AND " + ChampsCouche[14] + " = " + dl[5] + " AND " + ChampsCouche[3] + " > '" + Sql_obstimeGMT11SeuilH + "' AND "  + ChampsCouche[3] + " < '" + Sql_obstimeGMT11 + "'"
                            arcpy.MakeFeatureLayer_management(LienSDE + NomCouchePts, NomCouchePts)
                            PtsSim = [list(ps) for ps in arcpy.da.SearchCursor(NomCouchePts,[ChampsCouche[13], ChampsCouche[14], ChampsCouche[3]], SqlFiltreSeuilH)] #lon lat DATE, obstimeJou
                            #troisieme verification: s'il n'y a pas deja un point identique dans le seuil Horaire
                            if len(PtsSim) == 0:
                                verifline = 4
                            if verifline == 4:
                                #quatrieme verification si la surface intersecte on decoupe en fonction de Contour sans Buffer si aucune intersection le point et la surface sont elagues
                                surf_prj2 = PointToGeom(pt_prj2,prj2)
                                nbgc = len(GeomsContour)
                                GCdist = list()
                                for gc in GeomsContour:
                                    if surf_prj2.area != 0:
                                        if surf_prj2.distanceTo(gc) == 0:
                                            surf_prj2 = surf_prj2.intersect(gc, 4)
                                        else:
                                            GCdist.append(gc)
                                nbgcd = len(GCdist)
                                #200 000 m2 soit inferieur a 5% d'un pixel entier --- PARAMETRE EN DUR et qu'il a au moins intersecte une geometrie de la couche Contour
                                if surf_prj2.area > 200000 and nbgc != nbgcd:
                                    arcpy.MakeFeatureLayer_management(LienSDE + NomCouchePol, NomCouchePol)
                                    verifline = 5
                                    #rowpt enregistrement pour le point
                                    #rowpol enregistrement pour la surface
                                    rowpt.append(pt_prj2)
                                    rowpol.append(surf_prj2)
                                    rowpt.append(obstime)
                                    rowpol.append(obstime)
                                    del obstime
                                    rowpt.append("Himawari-8")  # sat
                                    rowpol.append("Himawari-8") #sat
                                    rowpt.append(obstimeGMT11)  # obstimeGMT11
                                    rowpt.append(datetime.datetime(obstimeGMT11.year, obstimeGMT11.month,
                                                                   obstimeGMT11.day))  # obstimeJour
                                    rowpol.append(datetime.datetime(obstimeGMT11.year, obstimeGMT11.month,
                                                                    obstimeGMT11.day))  # obstimeJour
                                    rowpt.append(pt_prj2[0].X)
                                    rowpt.append(pt_prj2[0].Y)
                                    rowpol.append(pt_prj2[0].X)
                                    rowpol.append(pt_prj2[0].Y)
                                    rowpt.append(int(dl[8]))  #  Volcano sur V2
                                    rowpt.append(int(dl[9]))  #  level sur V2
                                    rowpt.append(dl[10])  # reliabilty
                                    rowpt.append(dl[11])  # FRP
                                    rowpt.append(dl[12])  # QF
                                    rowpt.append(dl[13]) #Hot
                                    rowpt.append(pt[0].X) #lon
                                    rowpt.append(pt[0].Y) # lat
                                    rowpol.append(int(dl[8]))  # Volcano sur V2
                                    rowpol.append(int(dl[9]))  # level sur V2
                                    rowpol.append(dl[10])  # reliabilty
                                    rowpol.append(dl[11])  # FRP
                                    rowpol.append(dl[12])  # QF
                                    rowpol.append(dl[13])  # Hot
                                    rowpol.append(pt[0].X) #lon
                                    rowpol.append(pt[0].Y) # lat

                                    #element supplementaire pour la surface "superf_ha"
                                    rowpol.append(surf_prj2.area / 10000)
                                    #on implemente apres BegDate et EndDate pour le surfaces
                                    rowpol.append(obstimeGMT11)  # BegDate
                                    rowpol.append(obstimeGMT11)  # EndDate
                                    iCursPts = arcpy.da.InsertCursor(NomCouchePts, ChampsCouche)
                                    iCursPts.insertRow(rowpt)
                                    del iCursPts
                                    ChampsCouchePol = [c for c in ChampsCouche]
                                    ChampsCouchePol.append("superficie_ha")
                                    ChampsCouchePol.remove("DATE")
                                    ChampsCouchePol.append("BegDate")
                                    ChampsCouchePol.append("EndDate")
                                    iCursPol = arcpy.da.InsertCursor(NomCouchePol, ChampsCouchePol)
                                    iCursPol.insertRow(rowpol)
                                    del iCursPol
                                    Verif.append(verifline)
                                    arcpy.Delete_management(NomCouchePol)
                                else:
                                    Verif.append(verifline)
                            else:
                                Verif.append(verifline)
                            arcpy.Delete_management(NomCouchePts)
                        else:
                            Verif.append(verifline)
                    else:
                        Verif.append(verifline)
                #si a partir de la deuxieme ligne du fichier des lignes ne sont pas traitables
                elif i > 1 and len(dl) < 14:
                    verifline = 1
                    Verif.append(verifline)
            # apres avoir traite les lignes du fichier on envoit un email de rapport et on remplit la table de CSV traites
            HimawariEmailTableCSV(CSVFile, Verif, TableCSV, ChampTableCSV, idrepHeure)
            del Lines, NomCouchePts, NomCouchePol

        ###############fonction du traitement au niveau des fichiers CSV#########################
        def TraitementNiv4FichierCSV(repMois, repJour, repHeure, idrepHeure, repftpdep, ConnectFTP, DB, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, L_SDE):
            #On verifie si le repertoire est bien un repertoire car des soucis sont rencontres lorsqu'ils ne sont pas des repertoires grace a la commande MLST
            VerifType = ConnectFTP.sendcmd("MLST " + repftpdep + "/" + repMois + "/" + repJour + "/" + repHeure)
            if VerifType.find("type=dir") >= 0:
                ConnectFTP.sendcmd("CWD " + repftpdep + "/" + repMois + "/" + repJour + "/" + repHeure)
                CSVftp = list()
                ConnectFTP.dir(CSVftp.append)
                CSVftp = [c[c.find("H0"):] for c in CSVftp]
                arcpy.MakeTableView_management(rep + "/" + DB + "/" + TBCSV, TBCSV)
                for csvf in CSVftp:
                    CDB = [cdb[0] for cdb in arcpy.da.SearchCursor(TBCSV, [ChpTBCSV[0]], ChpTBCSV[2] + " = " + str(idrepHeure))]
                    if csvf not in CDB:
                        #on cree le fichier en local dans le repertoire DATA
                        os.chdir(repdata)
                        csv = open(repdata + "/" + csvf, "w")
                        ConnectFTP.retrbinary('RETR ' + csvf, csv.write)
                        csv.close()
                        #on corrige le CSV dans un nouveau csv _corrige.csv OBSOLETE plus besoin de transformer
                        #corrigecsvf = FormatageCSV(repdata, csvf)
                        ###CONTINUER ICI apres _corrige.csv
                        RecupDonneesCSV(repdata, csvf, layerPoint, layerPol, DB, ChpLayer, Etendue, TBCSV, ChpTBCSV, contour, contourBuf, codproj1, codproj2, seuilH, idrepHeure, L_SDE, transfproj)
                        #on supprime a la fin car temporaire
                        os.remove(repdata + "/" + csvf)
                        #os.remove(repdata + "/" + corrigecsvf)
                    elif csvf in CDB:
                        PrintEtLog("Le fichier CSV " + csvf + " est deja traite.")
                #Verification si le repertoire Heure est totalement traite
                CSVDB50mn = [list(cd) for cd in arcpy.da.SearchCursor(TBCSV, ChpTBCSV, ChpTBCSV[2] + " = " + str(idrepHeure) + " AND " + ChpTBCSV[0] + " LIKE '%" + repHeure + "50%'")]
                if len(CSVDB50mn) > 0:
                    VerificationHeure = 1
                else:
                    VerificationHeure = 0
                arcpy.Delete_management(TBCSV)
            else:
                objet = conf["SubjectMailAvert"]
                VerificationHeure = 2
                #envoi email vers jf.nguyenvansoc@oeil.nc et fabien.albouy@oeil.nc en avertissement
                fromaddr = FromADDR
                toaddr = ToADDR
                    #toaddr2 = ToADDR2
                msg = MIMEMultipart()
                msg['Subject'] = objet
                body = "Le lien suivant " + repftpdep + "/" + repMois + "/" + repJour + "/" + repHeure + " n'est pas un repertoire. Veuillez faire remonter cette information a Z-PTREE@ml.jaxa.jp"
                msg.attach(MIMEText(body, 'plain'))
                server = smtplib.SMTP(SMTP, SMTPPort)
                server.starttls()
                server.login(CompteEmail, MDPEmail)
                text = msg.as_string()
                server.sendmail(fromaddr, toaddr, text)
                #server.sendmail(fromaddr, toaddr2, text)
                server.quit()
            return VerificationHeure

        ###############fonction du traitement au niveau des repertoires Heures#########################
        def TraitementNiv3Heure(repMois, repJour, idrepJour, repftpdep, ConnectionFTP, DB, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, lien_sde):
            ConnectionFTP.sendcmd("CWD " + repftpdep + "/" + repMois + "/" + repJour)
            HeuresRepFTP = list()
            ConnectionFTP.dir(HeuresRepFTP.append)
            HeuresRepFTP = [hr[-2:] for hr in HeuresRepFTP if hr[-2:].isdigit()]
            arcpy.MakeTableView_management(rep + "/" + DB + "/" + TBHeure, TBHeure)
            HeuresDB = [list(h) for h in arcpy.da.SearchCursor(TBHeure, ChpTBHeure, ChpTBHeure[2] + " = " + str(idrepJour))]
            for hrep in HeuresRepFTP:
                HDB = [hd[0] for hd in arcpy.da.SearchCursor(TBHeure,[ChpTBHeure[0]], ChpTBHeure[2] + " = " + str(idrepJour))]
                nbh = len([hd[0] for hd in arcpy.da.SearchCursor(TBHeure,[ChpTBHeure[0]])])
                #cas si nouveau repertoire niveau heure
                ###EXCEPTION A GERER
                if hrep not in HDB:
                    iCurs = arcpy.da.InsertCursor(TBHeure, ChpTBHeure)
                    NewHDB = [hrep, nbh, idrepJour, 0]
                    iCurs.insertRow(NewHDB)
                    del iCurs
                    PrintEtLog("Traitement du repertoire Heure " + hrep + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                    VerifHeure = TraitementNiv4FichierCSV(repMois, repJour, hrep, nbh, repftpdep, ConnectionFTP, DB, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, lien_sde)
                    uCurs = arcpy.da.UpdateCursor(TBHeure, ChpTBHeure, ChpTBHeure[0] + " = '" + hrep + "'")
                    if VerifHeure == 1:
                        for u in uCurs:
                            u[3] = 1
                            uCurs.updateRow(u)
                        del uCurs
                        PrintEtLog("repertoire Heure " + hrep + " est passe en statut totalement traite" + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                    #ici code 2 ce n'est pas un repertoire on met le code 2
                    elif VerifHeure == 2:
                        for u in uCurs:
                            u[3] = 2
                            uCurs.updateRow(u)
                    else:
                        for u in uCurs:
                            u[3] = 0
                            uCurs.updateRow(u)
                        del uCurs
                elif hrep in HDB:
                    HDBFiltre = [hdbf for hdbf in HeuresDB if hdbf[0] == hrep]
                    HDBFiltre = HDBFiltre[0]
                    #si deja traite
                    if HDBFiltre[3] == 1 or HDBFiltre[3]  == 2:
                        PrintEtLog("Le repertoire de l'heure " + hrep + " est deja traite entierement")
                    #sinon on traite
                    else:
                        PrintEtLog("Traitement du repertoire Heure " + hrep + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        VerifHeure = TraitementNiv4FichierCSV(repMois, repJour, hrep, HDBFiltre[1], repftpdep, ConnectionFTP, DB, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, lien_sde)
                        uCurs = arcpy.da.UpdateCursor(TBHeure, ChpTBHeure, ChpTBHeure[0]  + " = '" + hrep + "'")
                        if VerifHeure == 1:
                            for u in uCurs:
                                u[3] = 1
                                uCurs.updateRow(u)
                            del uCurs
                            PrintEtLog("repertoire Heure " + hrep + " est passe en statut totalement traite"+ " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        elif VerifHeure == 2:
                            for u in uCurs:
                                u[3] = 2
                                uCurs.updateRow(u)
                            del uCurs
                            PrintEtLog("repertoire Heure " + hrep + " est passe en statut totalement traite"+ " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        else:
                            for u in uCurs:
                                u[3] = 0
                                uCurs.updateRow(u)
                            del uCurs
            # On verifie l'etat des repertoires heures pour le jour entier et voir si on peut considerer le jour comme Traite = 1 ou 0 puis mis en return
            Heure23DB = [list(h) for h in arcpy.da.SearchCursor(TBHeure, ChpTBHeure, ChpTBHeure[0] + " = '23' AND " + ChpTBHeure[2] + " = " + str(idrepJour))]
            arcpy.Delete_management(TBHeure)
            if len(Heure23DB) > 0:
                Heure23DB = Heure23DB[0]
                if Heure23DB[3] == 1 or Heure23DB[3] == 2:
                    VerificationJour = 1
                else:
                    VerificationJour = 0
            else:
                VerificationJour = 0
            return VerificationJour

        ###############fonction du traitement au niveau des repertoires Jours#########################
        def TraitementNiv2Jour(repMois, idrepMois, repftpdep, DB, TBJour, ChpTBJour, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj ,hote, uid, psw, liensde):
            connectionFTP = ftplib.FTP(hote, uid, psw)
            connectionFTP.sendcmd("CWD " + repftpdep + "/" + repMois)
            JoursRepFTP = list()
            connectionFTP.dir(JoursRepFTP.append)
            JoursRepFTP = [jr[-2:] for jr in JoursRepFTP if jr[-2:].isdigit()]
            arcpy.MakeTableView_management(rep + "/" + DB + "/" + TBJour, TBJour)
            JoursDB = [list(j) for j in arcpy.da.SearchCursor(TBJour, ChpTBJour, ChpTBJour[2] + " = " + str(idrepMois))]
            for jrep in JoursRepFTP:
                JDB = [jd[0] for jd in arcpy.da.SearchCursor(TBJour, [ChpTBJour[0]], ChpTBJour[2] + " = " + str(idrepMois))]
                nbj = len([jd[0] for jd in arcpy.da.SearchCursor(TBJour, [ChpTBJour[0]])])
                # cas si nouveau repertoire niveau jour
                if jrep not in JDB:
                    iCurs = arcpy.da.InsertCursor(TBJour, ChpTBJour)
                    NewJDB = [jrep, nbj, idrepMois, 0]
                    iCurs.insertRow(NewJDB)
                    del iCurs
                    PrintEtLog("Traitement du repertoire Jour " + jrep+ " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                    VerifJour = TraitementNiv3Heure(repMois,jrep, nbj, repftpdep, connectionFTP, DB, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, liensde)
                    uCurs = arcpy.da.UpdateCursor(TBJour, ChpTBJour, ChpTBJour[0] + " = '" + jrep + "'")
                    if VerifJour == 1:
                        for u in uCurs:
                            u[3] = 1
                            uCurs.updateRow(u)
                        del uCurs
                        PrintEtLog("repertoire Jour " + jrep + " est passe en statut totalement traite"+ " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                    else:
                        for u in uCurs:
                            u[3] = 0
                            uCurs.updateRow(u)
                        del uCurs

                # cas si deja existant
                elif jrep in JDB:
                    JourDBFiltre = [jdb for jdb in JoursDB if jdb[0] == jrep]
                    JourDBFiltre = JourDBFiltre[0]
                    if JourDBFiltre[3] == 1:
                        PrintEtLog("Le repertoire du jour " + jrep + " est deja traite entierement")
                    else:
                        PrintEtLog("Traitement du repertoire Jour " + jrep)
                        VerifJour = TraitementNiv3Heure(repMois, jrep, JourDBFiltre[1], repftpdep, connectionFTP, DB, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, liensde)
                        uCurs = arcpy.da.UpdateCursor(TBJour, ChpTBJour, ChpTBJour[0] + " = '" + jrep + "'")
                        if VerifJour == 1:
                            for u in uCurs:
                                u[3] = 1
                                uCurs.updateRow(u)
                            del uCurs
                            PrintEtLog("repertoire Jour " + jrep + " est passe en statut totalement traite" + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        else:
                            for u in uCurs:
                                u[3] = 0
                                uCurs.updateRow(u)
                            del uCurs
            connectionFTP.close()
            #On verifie l'etat des repertoires Jour pour le mois entier et voir si on peut considerer le mois comme Traite = 1 ou 0 puis mis en return
            JoursDB = [list(j) for j in arcpy.da.SearchCursor(TBJour, ChpTBJour, ChpTBJour[2] + " = " + str(idrepMois))]
            arcpy.Delete_management(TBJour)
            VerificationMois = VerificationTraitementMois(repMois, JoursDB)
            return VerificationMois


        ###############fonction du traitement au niveau des repertoires des Mois#########################
        def TraitementNiv1Mois(hostFTP,UIDFTP,PSWDFTP, repftpdep, DB, TBMois, ChpTBMois, TBJour, ChpTBJour, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, lienSDE):
            #si la table RepertoiresTraites est vide
            connect = ftplib.FTP(hostFTP, UIDFTP, PSWDFTP)
            connect.sendcmd("CWD " + repftpdep)
            ####Reconstruire Modele de donnees Repertoire avec une table pour le mois avec id idem pour les autres niveaux et lies les tables entre elles
            MoisRepFTP = list()
            connect.dir(MoisRepFTP.append)
            #on ignore le premier repertoire 07 (mauvais)
            MoisRepFTP.remove(MoisRepFTP[0])
            MoisRepFTP = [mr[-6:] for mr in MoisRepFTP if mr[-6:].isdigit()]
            #rep est hors parametres mais variable globale declaree en debut de script
            arcpy.MakeTableView_management(rep + "/" + DB + "/" + TBMois, TBMois)
            connect.close()
            for mrep in MoisRepFTP:
                MoisDB = [m[0] for m in arcpy.da.SearchCursor(TBMois, ChpTBMois)]
                nbrep = len(MoisDB)
                #cas si nouveau repertoire niveau mois
                if mrep not in MoisDB:
                    iCurs = arcpy.da.InsertCursor(TBMois, ChpTBMois)
                    #reactualisation pour prendre le nombre actuel et on ne se base pas sur MoisDB
                    NewRepDB = [mrep, nbrep, 0]
                    iCurs.insertRow(NewRepDB)
                    del iCurs
                    PrintEtLog("Traitement du repertoire Mois " + mrep + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                    VerifMois = TraitementNiv2Jour(mrep, nbrep, repftpdep, DB, TBJour, ChpTBJour, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, hostFTP, UIDFTP, PSWDFTP, lienSDE)
                    if VerifMois == 1:
                        uCurs = arcpy.da.UpdateCursor(TBMois,ChpTBMois,ChpTBMois[0] + " = '" + mrep + "'")
                        for u in uCurs:
                            u[2] = 1
                            uCurs.updateRow(u)
                        del uCurs
                        PrintEtLog("repertoire Mois " + mrep + " est passe en statut totalement traite "+ datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                    else:
                        for u in uCurs:
                            u[2] = 0
                            uCurs.updateRow(u)
                        del uCurs
                #cas si deja existant
                elif mrep in MoisDB:
                    MoisDBfiltre = [list(m) for m in arcpy.da.SearchCursor(TBMois, ChpTBMois, ChpTBMois[0] + " = '" + mrep + "'")]
                    MoisDBfiltre = MoisDBfiltre[0]
                    # si deja traite
                    if MoisDBfiltre[2] == 1:
                        PrintEtLog("Le repertoire du mois " + mrep + " est deja traite entierement")
                    #sinon on lance le traitement niv2 repertoire jour
                    else:
                        PrintEtLog("Traitement du repertoire Mois " + mrep + " " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        VerifMois = TraitementNiv2Jour(mrep, MoisDBfiltre[1], repftpdep, DB, TBJour, ChpTBJour, TBHeure, ChpTBHeure, TBCSV, ChpTBCSV, repdata, layerPol, layerPoint, ChpLayer, Etendue, contour, contourBuf, seuilH, codproj1, codproj2, transfproj, hostFTP, UIDFTP, PSWDFTP, lienSDE)
                        uCurs = arcpy.da.UpdateCursor(TBMois, ChpTBMois, ChpTBMois[0] + " = '" + mrep + "'")
                        if VerifMois == 1:
                            for u in uCurs:
                                u[2] = 1
                                uCurs.updateRow(u)
                            del uCurs
                            PrintEtLog("repertoire Mois " + mrep + " est passe en statut totalement traite "+ datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
                        else:
                            for u in uCurs:
                                u[2] = 0
                                uCurs.updateRow(u)
                            del uCurs
            arcpy.Delete_management(TBMois)

        ##########################PARAMETRES RECUPERES DE CONFIG.JSON###############################################
        ##REMPLACEMENT CSV PAR TABLE CSV TRAITES
        js = open(rep + "/Config.json","r")
        conf = json.load(js)
        TableCSV = conf["TableCSV"]
        ChampTableCSV = conf["ChampTableCSV"].split(";")
        TableRepMois = conf["TableRepMois"]
        ChampsTableRepMois = conf["ChampsTableRepMois"].split(";")
        TableRepJour = conf["TableRepJour"]
        ChampsTableRepJour = conf["ChampsTableRepJour"].split(";")
        TableRepHeure = conf["TableRepHeure"]
        ChampsTableRepHeure = conf["ChampsTableRepHeure"].split(";")
        GDB = conf["Gdb"]
        host = conf["hostFTP"]
        UID = conf["UIDFTP"]
        PSWD = conf["PswdFTP"]
        RepFTPDepart = conf["RepFTPDepart"]
        repData = rep + "/" + conf["repData"]
        LayerPolyg = conf["LayerPolyg"]
        LayerPoint = conf["LayerPoint"]
        Chps = conf["Chps"].split(";")
        TypeChps = conf["TypeChps"].split(";")
        EtendueNC = conf["EtendueNC"].split(";")
        SDEJeuData = conf["SDEJeuData"]
        PointPixelAlerteInc = conf["PointAlerteInc"]
        PixelAlerteInc = conf["PixelAlerteInc"]
        repContour = conf["repContour"]
        contourNC = conf["contourNC"]
        contourNCHimawari = conf["contourNCHimawari"]
        SeuilHoraire = conf["SeuilHoraire"]
        codeProj1 = conf["codeProj1"] #EPSG WGS1984
        codeProj2 = conf["codeProj2"] #EPSG RGNC
        TransformationProj = conf["TransformationProj"] #Transformation Projection
        js.close()
        ##########################EXECUTION###############################################
        PrintEtLog("Debut du traitement" + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        #Recuperation des enregistrements de la table des repertoires traites
        #####A METTRE A JOUR APRES FINALISATION FONCTION TRAITEMENTREP #####
        TraitementNiv1Mois(host, UID, PSWD, RepFTPDepart, GDB, TableRepMois, ChampsTableRepMois, TableRepJour, ChampsTableRepJour, TableRepHeure, ChampsTableRepHeure, TableCSV, ChampTableCSV, repData, PixelAlerteInc, PointPixelAlerteInc, Chps, EtendueNC, contourNC, contourNCHimawari, SeuilHoraire, codeProj1, codeProj2, TransformationProj, SDEJeuData)
        #on vide le repertoire DATA
        FichiersTelecharges = os.listdir(repData)
        if len(FichiersTelecharges) > 0:
            for ft in FichiersTelecharges:
                os.remove(repData + "/" + ft)
        del FichiersTelecharges
        ###FIN MISE A JOUR####
        log.write("fin du processus: " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        print("fin du processus: " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        log.close()
        del conf
    except Exception as e:
         try:
             log.write(str(e))
             log.close()
         except NameError:
             print("log n'a pas ete cree")
         try:
             if arcpy.Exists(TableCSV):
                 arcpy.Delete_management(TableCSV)
         except NameError:
             print("TableCSV n'existe pas")
         print(str(e))
         #envoi email vers jf.nguyenvansoc@oeil.nc
         msg = MIMEMultipart()
         msg['Subject'] = conf["SubjectMailError"]
         body = "l'erreur suivante a ete generee sur le script "+ rep + "/" + nomscript + ".py: Type: Exception: " + str(e)
         msg.attach(MIMEText(body, 'plain'))
         server = smtplib.SMTP(conf["SMTP"], conf["SMTPPort"])
         server.starttls()
         server.login(conf["CompteEmail"], conf["MDPEmail"])
         text = msg.as_string()
         server.sendmail(conf["fromaddr"], conf["toaddr"], text)
         #server.sendmail(conf["fromaddr"], conf["toaddr2"], text)
         server.quit()

except (IOError, NameError) as e:
    try:
         log.write(str(e))
         log.close()
    except NameError:
         print("log n'a pas ete cree")
    try:
         if arcpy.Exists(origlayer):
             arcpy.Delete_management(origlayer)
    except NameError:
         print("origlayer n'existe pas")
    try:
         if arcpy.Exists(dtselect):
             arcpy.Delete_management(dtselect)
    except NameError:
         print("dtselect n'existe pas")
    try:
         if arcpy.Exists(resultlayer):
             arcpy.Delete_management(resultlayer)
    except NameError:
         print("resultlayer n'existe pas")
    try:
         if arcpy.Exists(resultSurflayer):
             arcpy.Delete_management(resultSurflayer)
    except NameError:
        print("resultSurflayer n'existe pas")
    print(str(e))
     #envoi email vers jf.nguyenvansoc@oeil.nc
    msg = MIMEMultipart()
    msg['Subject'] = conf["SubjectMailError"]
    body = "l'erreur suivante a ete generee sur le script "+ rep + "/" + nomscript + ".py: Type: Exception: " + str(e)
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP(conf["SMTP"], conf["SMTPPort"])
    server.starttls()
    server.login(conf["CompteEmail"], conf["MDPEmail"])
    text = msg.as_string()
    server.sendmail(conf["fromaddr"], conf["toaddr"], text)
    #server.sendmail(conf["fromaddr"], conf["toaddr2"], text)
    server.quit()
